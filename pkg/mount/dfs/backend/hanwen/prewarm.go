//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"strings"
	"time"

	"github.com/rs/zerolog"
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

	// minElapsedForPrewarm guards against single-offset probes (ffprobe
	// metadata/subtitle-sync tools like Bazarr, decypharr's own repair engine,
	// media-server library scanners) that seek straight to a computed offset
	// and read there. Byte-volume-based guards were tried and both failed in
	// practice: comparing against total file size falsely blocked genuine
	// playback after a Handle reopen (a seek past the halfway point makes
	// the remaining bytes physically incapable of ever reaching 50% of the
	// whole file), while comparing against bytes-up-to-current-offset let a
	// real scan through (some tools, e.g. audio-sync analysis, genuinely
	// read a large contiguous span, not a single small chunk). Wall-clock
	// time elapsed since the file was opened is the actually robust signal:
	// a real viewer takes real minutes to reach 70% through any normal
	// episode, while an automated tool reads gigabytes in seconds regardless
	// of file size, bitrate, or how much of the file it happens to touch.
	minElapsedForPrewarm = 3 * time.Minute

	// prewarmFraction is how much of the next episode gets fetched: enough to
	// eliminate the "loading" moment when the user actually gets there,
	// without paying for a whole episode nobody may watch. Existing cache
	// expiry (FuseConfig.CacheExpiry) cleans it up if they don't continue.
	prewarmFraction = 0.20

	// prewarmMaxBytes caps a single prewarm fetch regardless of episode size.
	// A flat cap rather than bitrate-aware: for any real episode over ~1.25GB
	// (virtually all of them - a typical 1080p episode alone is ~1.5GB), this
	// cap is what actually limits the fetch, not prewarmFraction. Fine as a
	// first pass; a future version could make this user-configurable instead
	// of a hardcoded constant.
	prewarmMaxBytes = 256 * 1024 * 1024 // 256MB

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
	if fh.openedAt.IsZero() || time.Since(fh.openedAt) < minElapsedForPrewarm {
		return
	}
	if !fh.prewarmTriggered.CompareAndSwap(false, true) {
		return
	}
	// A plain, non-deduped logger: maybeTriggerPrewarm's CAS above already
	// guarantees this whole function runs at most once per open file, so
	// there's no log-spam risk here that would call for fh.logger's
	// rate-limiting. Using it directly was a bug - it dedupes by a fixed
	// per-file key across ALL of that file's log calls regardless of
	// message content, so every message here after the first within the
	// same window was being silently swallowed as a "duplicate".
	plainLog := logger.Default()
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
			plainLog.Debug().Str("file", name).Msg("Prewarm: not detected as a TV episode, skipping")
			return
		}
	}

	plainLog.Info().Str("file", name).Int("season", current.Season).Int("episode", current.EpStart).
		Msg("Prewarm: playback threshold crossed, looking for next episode")

	next := findNextEpisode(mgr, current)
	if next == nil {
		plainLog.Info().Str("file", name).Int("season", current.Season).Int("episode", current.EpStart+1).
			Msg("Prewarm: no matching next episode found among active torrents/jobs")
		return
	}

	vfsMgr := fh.file.vfs
	plainLog.Info().Str("file", name).Str("nextFile", next.Name()).Msg("Prewarm: starting fetch of next episode")
	go prewarmFile(vfsMgr, next, plainLog)
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
		// Fast pre-filter: skip on a confident season mismatch only - NOT on
		// title, and NOT on IsTV/season-zero. A season-pack folder name with
		// no episode number in it (e.g. "Show.S09.2021.WEB-DL...", no "E##")
		// is a real, common release-naming pattern that the parser can fail
		// on: it may not recognize "S09" as a season marker at all without an
		// accompanying episode number, dumping it straight into Title as
		// garbage text instead ("Show S09 2021"). That garbled title would
		// then never match the correctly-parsed per-file title and the whole
		// folder gets skipped before ever reaching its children - even though
		// the files inside parse perfectly fine on their own and would have
		// matched correctly. Only a *confident* season-number mismatch is
		// treated as authoritative here; everything else drills into
		// children, where the accurate per-file title/season/episode check
		// (below) is the real decider.
		if entryParsed.IsTV && entryParsed.Season != 0 && entryParsed.Season != wantSeason {
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
func prewarmFile(vfsMgr *vfs.Manager, info *manager.FileInfo, log zerolog.Logger) {
	sf, err := vfsMgr.GetFile(info)
	if err != nil {
		log.Warn().Str("file", info.Name()).Err(err).Msg("Prewarm: failed to open next episode")
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

	if readErr != nil && off < want {
		log.Warn().Str("file", info.Name()).Int64("bytes", off).Int64("wanted", want).Err(readErr).
			Msg("Prewarm: stopped early")
		return
	}
	log.Info().Str("file", info.Name()).Int64("bytes", off).Msg("Prewarm: complete")
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
