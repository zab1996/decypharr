//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

const (
	// prewarmThreshold is how far into a stream (by byte offset, not real
	// playtime — decypharr has no video-duration awareness, and doesn't need
	// it here) playback must reach before the next episode gets prewarmed.
	prewarmThreshold = 0.70

	// prewarmFraction is how much of the next episode gets fetched: enough to
	// eliminate the "loading" moment when the user actually gets there,
	// without paying for a whole episode nobody may watch. Existing cache
	// expiry (FuseConfig.CacheExpiry) cleans it up if they don't continue.
	prewarmFraction = 0.20

	// prewarmMaxBytes caps a single prewarm fetch regardless of episode size.
	prewarmMaxBytes = 64 * 1024 * 1024 // 64MB

	prewarmTimeout = 30 * time.Second

	// minPlausibleEpisodeBytes filters out sidecar files (subtitles etc.) that
	// share an episode's SxxExx naming but obviously aren't the video itself.
	minPlausibleEpisodeBytes = 20 * 1024 * 1024 // 20MB
)

// maybeTriggerPrewarm checks whether playback has crossed prewarmThreshold
// and, if so and this is the first crossing for this Handle, kicks off a
// background prewarm of the next episode. Safe to call on every read.
func (fh *Handle) maybeTriggerPrewarm(readTo int64) {
	if fh.file == nil || fh.file.config == nil || !fh.file.config.PrewarmNextEpisode {
		return
	}
	if fh.streamFile == nil {
		return
	}
	size := fh.streamFile.Size()
	if size <= 0 || float64(readTo)/float64(size) < prewarmThreshold {
		return
	}
	if !fh.prewarmTriggered.CompareAndSwap(false, true) {
		return
	}
	rlLogger := fh.logger
	name := fh.file.info.Name()

	if fh.file.vfs == nil {
		return
	}
	mgr := fh.file.vfs.Manager()
	if mgr == nil {
		return
	}

	current := utils.ParseTorrentName(name)
	if !current.IsTV || current.Season == 0 || current.EpStart == 0 {
		// Fall back to the parent torrent's name — a season-pack's own file
		// name sometimes lacks a title (e.g. a bare "S01E05.mkv"), so borrow
		// the torrent-level name for title/season context when needed.
		parentParsed := utils.ParseTorrentName(fh.file.info.Parent())
		if current.Title == "" {
			current.Title = parentParsed.Title
		}
		if current.Season == 0 {
			current.Season = parentParsed.Season
		}
		if !current.IsTV || current.Season == 0 || current.EpStart == 0 {
			if rlLogger != nil {
				rlLogger.Debug().Str("file", name).Msg("Prewarm: not detected as a TV episode, skipping")
			}
			return
		}
	}

	if rlLogger != nil {
		rlLogger.Info().Str("file", name).Int("season", current.Season).Int("episode", current.EpStart).
			Msg("Prewarm: playback threshold crossed, looking for next episode")
	}

	next := findNextEpisode(mgr, current)
	if next == nil {
		if rlLogger != nil {
			rlLogger.Info().Str("file", name).Int("season", current.Season).Int("episode", current.EpStart+1).
				Msg("Prewarm: no matching next episode found among active torrents/jobs")
		}
		return
	}

	vfsMgr := fh.file.vfs
	if rlLogger != nil {
		rlLogger.Info().Str("file", name).Str("nextFile", next.Name()).Msg("Prewarm: starting fetch of next episode")
	}
	go prewarmFile(vfsMgr, next, rlLogger)
}

// findNextEpisode scans every active torrent/NZB job for a file matching the
// same show, same season, and the next episode number. This deliberately
// covers both season packs (the match can be a sibling file within the SAME
// torrent as `current`) and single-episode-per-torrent releases (the match
// is a file in a DIFFERENT torrent) with one pass — there's no cheaper
// special case for season packs since both shapes turn up in the same scan.
func findNextEpisode(mgr *manager.Manager, current utils.ParsedName) *manager.FileInfo {
	wantTitle := normalizeEpisodeTitle(current.Title)
	wantSeason := current.Season
	wantEp := current.EpStart + 1

	_, torrents := mgr.GetEntryChildren(manager.EntryAllFolder)
	for _, entry := range torrents {
		if !entry.IsDir() {
			continue
		}
		entryParsed := utils.ParseTorrentName(entry.Name())
		// Fast pre-filter: if the torrent's own name parses to a title and it
		// clearly isn't the same show, skip drilling into its file list.
		if entryParsed.Title != "" && wantTitle != "" && normalizeEpisodeTitle(entryParsed.Title) != wantTitle {
			continue
		}

		_, children := mgr.GetTorrentChildren(entry.Name())
		for i := range children {
			child := &children[i]
			if child.IsDir() {
				continue
			}
			// Sidecar files (subtitles, etc.) are often named after the episode
			// they belong to and would otherwise match the same S/E pattern -
			// a lower bound well under any real episode file rules them out
			// without needing decypharr's video-extension allowlist here.
			if child.Size() < minPlausibleEpisodeBytes {
				continue
			}
			cp := utils.ParseTorrentName(child.Name())
			title := cp.Title
			if title == "" {
				title = entryParsed.Title
			}
			season := cp.Season
			if season == 0 {
				season = entryParsed.Season
			}
			if !cp.IsTV || season != wantSeason || cp.EpStart != wantEp {
				continue
			}
			if wantTitle != "" && normalizeEpisodeTitle(title) != wantTitle {
				continue
			}
			return child
		}
	}
	return nil
}

// prewarmFile fetches the first prewarmFraction (capped at prewarmMaxBytes)
// of `info` into cache, reusing the exact same open/read/close path a real
// playback request uses (vfs.Manager.GetFile + StreamingFile.ReadAtContext) —
// no new fetch or caching logic, just triggering the existing one early.
func prewarmFile(vfsMgr *vfs.Manager, info *manager.FileInfo, rlLogger *logger.RateLimitedEvent) {
	sf, err := vfsMgr.GetFile(info)
	if err != nil {
		if rlLogger != nil {
			rlLogger.Warn().Str("file", info.Name()).Err(err).Msg("Prewarm: failed to open next episode")
		}
		return
	}
	defer func() {
		sf.Close()
		vfsMgr.ReleaseFile(info)
	}()

	size := sf.Size()
	if size <= 0 {
		return
	}
	want := int64(float64(size) * prewarmFraction)
	if want > prewarmMaxBytes {
		want = prewarmMaxBytes
	}
	if want <= 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), prewarmTimeout)
	defer cancel()

	const chunk = 8 * 1024 * 1024 // 8MB per read call
	buf := make([]byte, chunk)
	var off int64
	var readErr error
	for off < want {
		readSize := chunk
		if remaining := want - off; remaining < int64(readSize) {
			readSize = int(remaining)
		}
		n, rerr := sf.ReadAtContext(ctx, buf[:readSize], off)
		off += int64(n)
		if rerr != nil {
			readErr = rerr
			break
		}
		if n == 0 {
			break
		}
	}

	if rlLogger == nil {
		return
	}
	if readErr != nil && off < want {
		rlLogger.Warn().Str("file", info.Name()).Int64("bytes", off).Int64("wanted", want).Err(readErr).
			Msg("Prewarm: stopped early")
		return
	}
	rlLogger.Info().Str("file", info.Name()).Int64("bytes", off).Msg("Prewarm: complete")
}

// normalizeEpisodeTitle makes two title strings comparable regardless of
// spacing/punctuation/case differences between release names.
func normalizeEpisodeTitle(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
