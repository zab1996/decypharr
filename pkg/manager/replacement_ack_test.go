package manager

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-manager-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestAcknowledgeReplacementPreservesSeasonPackSibling(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	entry := &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "old-uuid",
		Name:     "Old.Season.Pack",
		Files: map[string]*storage.File{
			"S01E01.mkv": {Name: "S01E01.mkv", InfoHash: "old-uuid", Size: 100},
			"S01E02.mkv": {Name: "S01E02.mkv", InfoHash: "old-uuid", Size: 100},
		},
		CliDebridIDs: map[string]int64{"S01E01.mkv": 101, "S01E02.mkv": 102},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveEntryHealth(&storage.EntryHealth{
		EntryName: "Old.Season.Pack", Protocol: config.ProtocolNZB, Status: storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: "Old.Season.Pack", FileName: "S01E01.mkv", InfoHash: "old-uuid",
			Protocol: config.ProtocolNZB, CliDebridID: 101, Reason: "media_probe_failed",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{storage: store}
	repair := &Repair{manager: mgr}
	result, err := repair.AcknowledgeReplacement(ReplacementAckRequest{
		EntryName: "Old.Season.Pack", FileName: "S01E01.mkv", InfoHash: "old-uuid",
		CliDebridID: 101, Reason: "media_probe_failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "removed" || result.EntryDeleted {
		t.Fatalf("result = %#v", result)
	}
	item, err := store.GetEntryItem("Old.Season.Pack")
	if err != nil {
		t.Fatal(err)
	}
	if !item.Files["S01E01.mkv"].Deleted || item.Files["S01E02.mkv"].Deleted {
		t.Fatalf("unexpected sibling state: %#v", item.Files)
	}

	// A repeated acknowledgement is harmless and does not touch the sibling.
	result, err = repair.AcknowledgeReplacement(ReplacementAckRequest{
		EntryName: "Old.Season.Pack", FileName: "S01E01.mkv", InfoHash: "old-uuid",
		CliDebridID: 101, Reason: "media_probe_failed",
	})
	if err != nil || result.Status != "already_removed" {
		t.Fatalf("repeat result = %#v, err=%v", result, err)
	}
}

func TestAcknowledgeReplacementReclaimsSupersededSlot(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	old := &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "old-uuid",
		Name:     "Bar.Rescue.S03E01",
		Files: map[string]*storage.File{
			"episode.mkv": {Name: "episode.mkv", InfoHash: "old-uuid", Size: 100, AddedOn: time.Unix(1000, 0)},
		},
		CliDebridIDs: map[string]int64{"episode.mkv": 55},
	}
	if err := store.AddOrUpdate(old); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveEntryHealth(&storage.EntryHealth{
		EntryName: "Bar.Rescue.S03E01", Protocol: config.ProtocolNZB, Status: storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: "Bar.Rescue.S03E01", FileName: "episode.mkv", InfoHash: "old-uuid",
			Protocol: config.ProtocolNZB, CliDebridID: 55, Reason: "media_probe_failed",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// Same folder name, same file name, but a newer replacement download
	// registered under the same cli_debrid_id — this is what updateEntryItem
	// does in place, in production, well before an exact-cleanup ack for the
	// old file can arrive.
	replacement := &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "new-uuid",
		Name:     "Bar.Rescue.S03E01",
		Files: map[string]*storage.File{
			"episode.mkv": {Name: "episode.mkv", InfoHash: "new-uuid", Size: 200, AddedOn: time.Unix(2000, 0)},
		},
		CliDebridIDs: map[string]int64{"episode.mkv": 55},
	}
	if err := store.AddOrUpdate(replacement); err != nil {
		t.Fatal(err)
	}
	item, err := store.GetEntryItem("Bar.Rescue.S03E01")
	if err != nil {
		t.Fatal(err)
	}
	if item.Files["episode.mkv"].InfoHash != "new-uuid" {
		t.Fatalf("expected slot to already be overwritten by the replacement, got %#v", item.Files["episode.mkv"])
	}

	mgr := &Manager{storage: store, config: config.Get()}
	mgr.initEntryCache()
	repair := &Repair{manager: mgr}
	result, err := repair.AcknowledgeReplacement(ReplacementAckRequest{
		EntryName: "Bar.Rescue.S03E01", FileName: "episode.mkv", InfoHash: "old-uuid",
		CliDebridID: 55, Reason: "media_probe_failed",
	})
	if err != nil {
		t.Fatalf("expected superseded slot to be treated as already_removed, got err=%v", err)
	}
	if result.Status != "already_removed" {
		t.Fatalf("result = %#v", result)
	}

	// The new file must be untouched — this must never delete the replacement.
	item, err = store.GetEntryItem("Bar.Rescue.S03E01")
	if err != nil {
		t.Fatal(err)
	}
	if item.Files["episode.mkv"].Deleted || item.Files["episode.mkv"].InfoHash != "new-uuid" {
		t.Fatalf("replacement file must survive untouched: %#v", item.Files["episode.mkv"])
	}

	// The orphaned old provider entry must be reclaimed.
	if _, err := mgr.GetEntry("old-uuid"); err == nil {
		t.Fatal("expected orphaned old provider entry to be deleted")
	}

	// The stale broken-health record for the old file must be cleared.
	health, err := store.GetEntryHealth("Bar.Rescue.S03E01")
	if err != nil {
		t.Fatal(err)
	}
	for _, broken := range health.BrokenFiles {
		if broken.InfoHash == "old-uuid" {
			t.Fatalf("expected old broken-health record to be cleared, still present: %#v", broken)
		}
	}
}

func TestAcknowledgeReplacementProceedsWhenIndexRegistrationOrphaned(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	old := &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "old-uuid",
		Name:     "Old.Release",
		Files: map[string]*storage.File{
			"episode.mkv": {Name: "episode.mkv", InfoHash: "old-uuid", Size: 100},
		},
		// The per-hash torrent-index record's registration is empty — desynced
		// from the entry-item store, as happens in production when this
		// record drifts out of the torrent listing while the entry-item
		// (and its health record) remain correct.
		CliDebridIDs: map[string]int64{},
	}
	if err := store.AddOrUpdate(old); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveEntryHealth(&storage.EntryHealth{
		EntryName: "Old.Release", Protocol: config.ProtocolNZB, Status: storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: "Old.Release", FileName: "episode.mkv", InfoHash: "old-uuid",
			Protocol: config.ProtocolNZB, CliDebridID: 55, Reason: "media_probe_failed",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{storage: store, config: config.Get()}
	mgr.initEntryCache()
	repair := &Repair{manager: mgr}
	result, err := repair.AcknowledgeReplacement(ReplacementAckRequest{
		EntryName: "Old.Release", FileName: "episode.mkv", InfoHash: "old-uuid",
		CliDebridID: 55, Reason: "media_probe_failed",
	})
	if err != nil {
		t.Fatalf("expected orphaned index registration not to block deletion, got err=%v", err)
	}
	if result.Status != "removed" {
		t.Fatalf("result = %#v", result)
	}
	// This was the only file in the entry, so the whole entry is removed,
	// not just marked deleted.
	if _, err := store.GetEntryItem("Old.Release"); err == nil {
		t.Fatal("expected entry to be fully removed after its last file was deleted")
	}
}

func TestAcknowledgeReplacementStillBlocksOnGenuineReassignment(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	old := &storage.Entry{
		Protocol: config.ProtocolNZB,
		InfoHash: "old-uuid",
		Name:     "Old.Release",
		Files: map[string]*storage.File{
			"episode.mkv": {Name: "episode.mkv", InfoHash: "old-uuid", Size: 100},
		},
		// Registered to a *different*, non-zero cli_debrid_id — genuine
		// evidence something else has claimed this slot, unlike an empty
		// registration. This must keep blocking as stale_target.
		CliDebridIDs: map[string]int64{"episode.mkv": 999},
	}
	if err := store.AddOrUpdate(old); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveEntryHealth(&storage.EntryHealth{
		EntryName: "Old.Release", Protocol: config.ProtocolNZB, Status: storage.HealthBroken,
		BrokenFiles: []storage.BrokenFile{{
			EntryName: "Old.Release", FileName: "episode.mkv", InfoHash: "old-uuid",
			Protocol: config.ProtocolNZB, CliDebridID: 55, Reason: "media_probe_failed",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	mgr := &Manager{storage: store, config: config.Get()}
	mgr.initEntryCache()
	repair := &Repair{manager: mgr}
	_, err = repair.AcknowledgeReplacement(ReplacementAckRequest{
		EntryName: "Old.Release", FileName: "episode.mkv", InfoHash: "old-uuid",
		CliDebridID: 55, Reason: "media_probe_failed",
	})
	var ackErr *ReplacementAckError
	if !errors.As(err, &ackErr) || ackErr.Code != "stale_target" {
		t.Fatalf("expected stale_target for genuine reassignment, got err=%v", err)
	}
	item, err := store.GetEntryItem("Old.Release")
	if err != nil {
		t.Fatal(err)
	}
	if item.Files["episode.mkv"].Deleted {
		t.Fatal("file must not be deleted while genuinely reassigned")
	}
}

func TestVerifyReplacementPersistsArticleFailure(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	entry := &storage.Entry{
		Protocol: config.ProtocolNZB, InfoHash: "new-uuid", Name: "Candidate.Release",
		Files: map[string]*storage.File{
			"episode.mkv": {Name: "episode.mkv", InfoHash: "new-uuid", Size: 100},
		},
		CliDebridIDs: map[string]int64{"episode.mkv": 77},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	repair := &Repair{
		manager: &Manager{storage: store}, mediaProbeSlots: make(chan struct{}, 4),
		replacementNZBProbe: func(_ context.Context, _ *storage.Entry, _ string, res fileResult) fileResult {
			res.broken = true
			res.reason = "usenet_segment_missing"
			return res
		},
	}
	result, err := repair.VerifyReplacement(context.Background(), ReplacementVerifyRequest{
		CliDebridID: 77, InfoHash: "new-uuid",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "broken" || result.Reason != "usenet_segment_missing" {
		t.Fatalf("result = %#v", result)
	}
	health, err := store.GetEntryHealth("Candidate.Release")
	if err != nil {
		t.Fatal(err)
	}
	if len(health.BrokenFiles) != 1 || health.BrokenFiles[0].CliDebridID != 77 {
		t.Fatalf("health = %#v", health)
	}
}

// TestVerifyReplacementSkipsFfprobeWhenSegmentsHealthy locks in that
// VerifyReplacement no longer runs its own mounted-media ffprobe check.
// That verification is cli_debrid's ffprobe_all_nzbs responsibility now —
// it already runs on the same replacement candidate as it passes through
// Adding→Checking, independent of why the item was added. Running it here
// too was a redundant, independent duplicate of the same check.
func TestVerifyReplacementSkipsFfprobeWhenSegmentsHealthy(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	entry := &storage.Entry{
		Protocol: config.ProtocolNZB, InfoHash: "new-uuid-2", Name: "Candidate.Release.2",
		Files: map[string]*storage.File{
			"episode.mkv": {Name: "episode.mkv", InfoHash: "new-uuid-2", Size: 100},
		},
		CliDebridIDs: map[string]int64{"episode.mkv": 88},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	probeCalled := false
	repair := &Repair{
		manager: &Manager{storage: store}, mediaProbeSlots: make(chan struct{}, 4),
		// Deliberately hostile: if VerifyReplacement still called this, the
		// result would come back broken instead of healthy, failing the
		// assertion below.
		mediaProbeAttempt: func(context.Context, string) mediaProbeResult {
			probeCalled = true
			return mediaProbeResult{state: mediaProbeBroken, reason: "media_probe_failed"}
		},
		replacementNZBProbe: func(_ context.Context, _ *storage.Entry, _ string, res fileResult) fileResult {
			res.healthy = true
			return res
		},
	}
	result, err := repair.VerifyReplacement(context.Background(), ReplacementVerifyRequest{
		CliDebridID: 88, InfoHash: "new-uuid-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "healthy" {
		t.Fatalf("result = %#v", result)
	}
	if probeCalled {
		t.Fatal("expected mediaProbeAttempt (ffprobe) to not be called by VerifyReplacement")
	}
}
