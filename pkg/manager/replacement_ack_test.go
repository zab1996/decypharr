package manager

import (
	"context"
	"os"
	"testing"

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
