package dfs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

// Prewarm-next-episode, driven by Plex's own /status/sessions API instead of
// inferring "playback" from raw FUSE read patterns. A prior version of this
// feature watched byte offsets on file reads directly, but that has no way
// to reliably tell a real viewer apart from any other tool that reads files
// (subtitle-sync scanners, decypharr's own repair engine, media-server
// library scans) - every heuristic tried (offset alone, cumulative bytes
// read, wall-clock time elapsed) kept producing false positives. Plex
// already knows, authoritatively, whether a session is a real client
// actively watching something and exactly how far into it they are -
// querying that directly sidesteps the whole class of problem.

const (
	plexPollInterval  = 20 * time.Second
	plexPrewarmThresh = 0.70

	prewarmFraction = 0.20
	prewarmMaxBytes = 256 * 1024 * 1024

	// prewarmTimeout is a safety backstop, not a speed estimate: without it,
	// a genuinely stalled download (network hiccup, provider outage) would
	// leave the prewarm goroutine blocked on ReadAtContext forever, quietly
	// holding a download slot/connection indefinitely. A background prewarm
	// doesn't need to finish fast - it just needs to eventually give up if
	// something's actually broken - so this is deliberately generous rather
	// than tuned to match typical download speed for the (always capped at
	// prewarmMaxBytes) fetch size.
	prewarmTimeout = 5 * time.Minute

	minPlausibleEpisodeSize = 20 * 1024 * 1024

	// prewarmedTTL bounds how long a session key is remembered as "already
	// prewarmed," so a session that ends and later restarts (or a different
	// session that happens to reuse a ratingKey after a Plex restart) isn't
	// permanently blocked.
	prewarmedTTL = 6 * time.Hour
)

type plexSessionsResponse struct {
	MediaContainer struct {
		Metadata []plexSession `json:"Metadata"`
	} `json:"MediaContainer"`
}

type plexSession struct {
	RatingKey        string `json:"ratingKey"`
	ParentRatingKey  string `json:"parentRatingKey"` // season's ratingKey - used to list the season's episodes
	Type             string `json:"type"`            // "episode", "movie", etc.
	GrandparentTitle string `json:"grandparentTitle"`
	ParentIndex      int    `json:"parentIndex"` // season number
	Index            int    `json:"index"`       // episode number
	ViewOffset       int64  `json:"viewOffset"`  // ms
	Duration         int64  `json:"duration"`    // ms
}

// plexChildrenResponse is the shape of /library/metadata/{ratingKey}/children.
type plexChildrenResponse struct {
	MediaContainer struct {
		Metadata []struct {
			Index int `json:"index"`
			Media []struct {
				Part []struct {
					File string `json:"file"`
				} `json:"Part"`
			} `json:"Media"`
		} `json:"Metadata"`
	} `json:"MediaContainer"`
}

// fetchNextEpisodeFilename asks Plex directly for the next episode within
// the same season (via the season's own ratingKey) and returns just the
// basename of the file Plex itself reports for it - authoritative, since
// Plex resolves "next episode" from its own TheTVDB/TMDB-backed metadata,
// not from parsing a release filename. Returns "" if Plex doesn't have a
// next episode in this season (e.g. a season finale) or the lookup fails.
func fetchNextEpisodeFilename(ctx context.Context, client *http.Client, plexURL, plexToken, seasonRatingKey string, wantIndex int) string {
	if seasonRatingKey == "" {
		return ""
	}
	url := strings.TrimRight(plexURL, "/") + "/library/metadata/" + seasonRatingKey + "/children"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("X-Plex-Token", plexToken)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	var parsed plexChildrenResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return ""
	}
	for _, ep := range parsed.MediaContainer.Metadata {
		if ep.Index != wantIndex {
			continue
		}
		if len(ep.Media) > 0 && len(ep.Media[0].Part) > 0 && ep.Media[0].Part[0].File != "" {
			return path.Base(ep.Media[0].Part[0].File)
		}
	}
	return ""
}

// findFileByBasename does an exact (case-insensitive) filename match across
// everything decypharr tracks. Used when Plex itself has already told us
// the exact next-episode filename - a plain string comparison, no title
// parsing or normalization involved, so it can't suffer the class of
// mismatch bugs that title-based matching (findNextEpisode) can.
func findFileByBasename(mgr *manager.Manager, basename string) *manager.FileInfo {
	if basename == "" {
		return nil
	}
	wantLower := strings.ToLower(basename)
	_, torrents := mgr.GetEntryChildren(manager.EntryAllFolder)
	for _, entry := range torrents {
		if !entry.IsDir() {
			continue
		}
		_, children := mgr.GetTorrentChildren(entry.Name())
		for i := range children {
			child := &children[i]
			if child.IsDir() {
				continue
			}
			if strings.ToLower(child.Name()) == wantLower {
				return child
			}
		}
	}
	return nil
}

// startPlexPrewarmPoller runs until ctx is cancelled. No-op unless both
// PrewarmNextEpisode is enabled and PlexURL/PlexToken are configured.
func (m *Manager) startPlexPrewarmPoller(ctx context.Context) {
	if !m.config.PrewarmNextEpisode {
		return
	}
	if m.config.PlexURL == "" || m.config.PlexToken == "" {
		m.logger.Warn().Msg("Prewarm: prewarm_next_episode is enabled but plex_url/plex_token are not set - nothing will happen")
		return
	}

	log := logger.Default()
	seen := &prewarmedSessions{entries: make(map[string]time.Time)}
	client := &http.Client{Timeout: 10 * time.Second}

	go func() {
		ticker := time.NewTicker(plexPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pollPlexSessionsOnce(ctx, client, m.config.PlexURL, m.config.PlexToken, m.manager, m.vfs, seen, log)
			}
		}
	}()
	log.Info().Str("plex_url", m.config.PlexURL).Dur("interval", plexPollInterval).Msg("Prewarm: started Plex session poller")
}

func pollPlexSessionsOnce(ctx context.Context, client *http.Client, plexURL, plexToken string, mgr *manager.Manager, vfsMgr *vfs.Manager, seen *prewarmedSessions, log zerolog.Logger) {
	sessions, err := fetchPlexSessions(ctx, client, plexURL, plexToken)
	if err != nil {
		log.Warn().Err(err).Msg("Prewarm: failed to fetch Plex sessions")
		return
	}
	seen.evictExpired()

	for _, s := range sessions {
		if s.Type != "episode" || s.Duration <= 0 || s.ParentIndex <= 0 || s.Index <= 0 || s.GrandparentTitle == "" {
			continue
		}
		if float64(s.ViewOffset)/float64(s.Duration) < plexPrewarmThresh {
			continue
		}
		if seen.markIfNew(s.RatingKey) {
			log.Info().Str("show", s.GrandparentTitle).Int("season", s.ParentIndex).Int("episode", s.Index).
				Msg("Prewarm: Plex session crossed threshold, looking for next episode")

			// Primary path: ask Plex directly for the next episode's exact
			// filename and match it verbatim - Plex resolves "next episode"
			// from its own metadata, no title parsing/normalization involved,
			// so it can't suffer the class of mismatch bug title matching can
			// (already hit twice tonight: a season-pack folder name PTT
			// couldn't parse, and a "(US)"-style disambiguator Plex keeps but
			// release filenames drop).
			var next *manager.FileInfo
			if nextName := fetchNextEpisodeFilename(ctx, client, plexURL, plexToken, s.ParentRatingKey, s.Index+1); nextName != "" {
				next = findFileByBasename(mgr, nextName)
			}
			// Fallback: title/season/episode-number matching, for when the
			// direct Plex lookup fails (network hiccup, season boundary
			// Plex's /children call doesn't cover, etc.) rather than giving
			// up entirely.
			if next == nil {
				current := utils.ParsedName{
					Title:   s.GrandparentTitle,
					Season:  s.ParentIndex,
					EpStart: s.Index,
					IsTV:    true,
				}
				next = findNextEpisode(mgr, current)
			}
			if next == nil {
				log.Info().Str("show", s.GrandparentTitle).Int("season", s.ParentIndex).Int("episode", s.Index+1).
					Msg("Prewarm: no matching next episode found among active torrents/jobs")
				continue
			}
			log.Info().Str("show", s.GrandparentTitle).Str("nextFile", next.Name()).Msg("Prewarm: starting fetch of next episode")
			go prewarmFile(vfsMgr, next, log)
		}
	}
}

func fetchPlexSessions(ctx context.Context, client *http.Client, plexURL, plexToken string) ([]plexSession, error) {
	url := strings.TrimRight(plexURL, "/") + "/status/sessions"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Plex-Token", plexToken)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("plex returned status %d", resp.StatusCode)
	}
	var parsed plexSessionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	return parsed.MediaContainer.Metadata, nil
}

// prewarmedSessions tracks which Plex session ratingKeys have already
// triggered a prewarm, so a session polled every 20s for the rest of an
// episode doesn't re-fetch the next episode repeatedly.
type prewarmedSessions struct {
	mu      sync.Mutex
	entries map[string]time.Time
}

func (p *prewarmedSessions) markIfNew(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.entries[key]; ok {
		return false
	}
	p.entries[key] = time.Now()
	return true
}

func (p *prewarmedSessions) evictExpired() {
	p.mu.Lock()
	defer p.mu.Unlock()
	cutoff := time.Now().Add(-prewarmedTTL)
	for k, t := range p.entries {
		if t.Before(cutoff) {
			delete(p.entries, k)
		}
	}
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
			if child.Size() < minPlausibleEpisodeSize {
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
	// Strip a trailing parenthetical disambiguator ("The Office (US)" ->
	// "The Office") before reducing to bare alphanumerics. Plex's
	// grandparentTitle keeps these (it uses them to distinguish shows with
	// the same name, e.g. US vs UK versions), but ParseTorrentName drops
	// them from release filenames - without stripping here, a session's
	// title ("The Office (US)" -> "theofficeus") would never match its own
	// tracked files' title ("The Office" -> "theoffice"), silently failing
	// every match for any show whose Plex title carries this kind of suffix.
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = s[:i]
	}
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
