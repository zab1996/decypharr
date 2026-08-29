package storage

import (
	"path/filepath"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func addTestEntry(t *testing.T, s *Storage, hash string, protocol config.Protocol, provider string, size int64) {
	t.Helper()
	entry := &Entry{
		InfoHash:       hash,
		Name:           hash,
		Protocol:       protocol,
		ActiveProvider: provider,
		Size:           size,
		Providers:      map[string]*ProviderEntry{},
		Files:          map[string]*File{},
	}
	if err := s.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate(%s): %v", hash, err)
	}
}

func TestContentStatsByProvider(t *testing.T) {
	s := newTestStorage(t)

	addTestEntry(t, s, "hash-rd-1", config.ProtocolTorrent, "realdebrid", 1000)
	addTestEntry(t, s, "hash-rd-2", config.ProtocolTorrent, "realdebrid", 2000)
	addTestEntry(t, s, "hash-tb-1", config.ProtocolTorrent, "torbox", 5000)
	// NZB entries must never be attributed to a debrid provider, even if
	// ActiveProvider happens to be set to something (e.g. "usenet").
	addTestEntry(t, s, "hash-nzb-1", config.ProtocolNZB, "usenet", 9999)
	// An entry with no ActiveProvider must be skipped, not bucketed under "".
	addTestEntry(t, s, "hash-no-provider", config.ProtocolTorrent, "", 4242)

	got := s.ContentStatsByProvider()

	if len(got) != 2 {
		t.Fatalf("expected 2 providers, got %d: %+v", len(got), got)
	}
	rd := got["realdebrid"]
	if rd == nil || rd.Count != 2 || rd.TotalSize != 3000 {
		t.Fatalf("realdebrid stats = %+v, want Count=2 TotalSize=3000", rd)
	}
	tb := got["torbox"]
	if tb == nil || tb.Count != 1 || tb.TotalSize != 5000 {
		t.Fatalf("torbox stats = %+v, want Count=1 TotalSize=5000", tb)
	}
	if _, ok := got["usenet"]; ok {
		t.Fatalf("NZB entry must not be attributed to a debrid provider: %+v", got)
	}
	if _, ok := got[""]; ok {
		t.Fatalf("entry with no ActiveProvider must not create a blank-key bucket: %+v", got)
	}
}

func TestNZBContentStats(t *testing.T) {
	s := newTestStorage(t)

	addTestEntry(t, s, "hash-nzb-1", config.ProtocolNZB, "usenet", 1000)
	addTestEntry(t, s, "hash-nzb-2", config.ProtocolNZB, "usenet", 2500)
	addTestEntry(t, s, "hash-torrent-1", config.ProtocolTorrent, "realdebrid", 999999)

	totalSize, count := s.NZBContentStats()
	if count != 2 || totalSize != 3500 {
		t.Fatalf("NZBContentStats() = (%d, %d), want (3500, 2)", totalSize, count)
	}
}
