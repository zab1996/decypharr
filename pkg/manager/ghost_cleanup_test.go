package manager

import (
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newTestManager builds a Manager suitable for exercising deleteGhostEntries
// in tests. clients must be a real xsync.Map (its zero value is an
// unusable nil pointer, unlike stdlib sync.Map) since Case 1's cleanup path
// calls ProviderClient() -> m.clients.Load(...) even when no provider is
// configured for the deleted entry.
func newTestManager(store *storage.Storage) *Manager {
	return &Manager{storage: store, clients: xsync.NewMap[string, debrid.Client]()}
}

// TestDeleteGhostEntries_DoesNotDeleteSoleCopyWhenNoRenamedCopyExists is a
// regression test: an Entry record can end up with Name != OriginalFilename
// (it was renamed at some point) without a live EntryItem actually existing
// under that new name — the rename creates a new EntryItem but never
// deletes the old one, and that new one can itself later be gone. Case 3
// must not delete the only remaining EntryItem just because the Entry
// record claims a rename happened.
func TestDeleteGhostEntries_DoesNotDeleteSoleCopyWhenNoRenamedCopyExists(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// Entry claims to have been renamed (Name != OriginalFilename), but no
	// EntryItem exists under Name — the "renamed copy" is not actually there.
	entry := &storage.Entry{
		Protocol: config.ProtocolNZB, InfoHash: "hash-1",
		Name: "Movie.2020.1080p.WEB-DL.CLEAN-GROUP", OriginalFilename: "raw.release.name.mkv",
		Files: map[string]*storage.File{
			"raw.release.name.mkv": {Name: "raw.release.name.mkv", InfoHash: "hash-1", Size: 100},
		},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}

	// AddOrUpdate's own updateEntryItem creates an EntryItem keyed by
	// entry.GetFolder() (based on entry.Name), not the raw filename — so
	// simulate the real-world case by also writing a raw-name EntryItem
	// pointing at the same file/hash, as if left behind by an earlier
	// rename whose new EntryItem has since been removed some other way.
	if err := store.UpdateItem(&storage.EntryItem{
		Name: "raw.release.name.mkv",
		Files: map[string]*storage.File{
			"raw.release.name.mkv": {Name: "raw.release.name.mkv", InfoHash: "hash-1", Size: 100},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// Remove whatever EntryItem AddOrUpdate created under the "renamed"
	// name, so no renamed copy actually exists — the exact scenario this
	// fix protects against.
	_ = store.DeleteEntryItemByName(entry.GetFolder())

	if _, err := store.GetEntryItem("raw.release.name.mkv"); err != nil {
		t.Fatalf("setup failed: raw-name EntryItem should exist before cleanup: %v", err)
	}

	mgr := newTestManager(store)
	mgr.deleteGhostEntries()

	if _, err := store.GetEntryItem("raw.release.name.mkv"); err != nil {
		t.Fatalf("sole remaining EntryItem must survive when no renamed copy exists, got err=%v", err)
	}
}

// TestDeleteGhostEntries_DeletesRawCopyWhenRenamedCopyGenuinelyExists confirms
// the cleanup still works for its actual intended case: a genuine leftover
// raw-name duplicate alongside a real, reachable renamed EntryItem for the
// same file/hash.
func TestDeleteGhostEntries_DeletesRawCopyWhenRenamedCopyGenuinelyExists(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	entry := &storage.Entry{
		Protocol: config.ProtocolNZB, InfoHash: "hash-2",
		Name: "Movie.2020.1080p.WEB-DL.CLEAN-GROUP", OriginalFilename: "raw.release.name2.mkv",
		Files: map[string]*storage.File{
			"raw.release.name2.mkv": {Name: "raw.release.name2.mkv", InfoHash: "hash-2", Size: 100},
		},
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	// AddOrUpdate already created the renamed EntryItem via updateEntryItem.
	if _, err := store.GetEntryItem(entry.GetFolder()); err != nil {
		t.Fatalf("setup failed: renamed EntryItem should exist: %v", err)
	}

	// Leftover raw-name EntryItem, as a real rename leaves behind.
	if err := store.UpdateItem(&storage.EntryItem{
		Name: "raw.release.name2.mkv",
		Files: map[string]*storage.File{
			"raw.release.name2.mkv": {Name: "raw.release.name2.mkv", InfoHash: "hash-2", Size: 100},
		},
	}); err != nil {
		t.Fatal(err)
	}

	mgr := newTestManager(store)
	mgr.deleteGhostEntries()

	if _, err := store.GetEntryItem("raw.release.name2.mkv"); err == nil {
		t.Fatal("leftover raw-name EntryItem should have been deleted when a genuine renamed copy exists")
	}
	if _, err := store.GetEntryItem(entry.GetFolder()); err != nil {
		t.Fatalf("renamed EntryItem must survive: %v", err)
	}
}

// TestDeleteGhostEntries_Case1DeletesRawEntryForNzbProtocol confirms Case 1
// (an EntryItem folder whose files span two distinct Entry hashes — one
// CLI-renamed, one still raw — where the raw one is a ghost duplicate of the
// renamed one) applies to nzb entries too, not just torrent. Both protocols
// share the same Entry/EntryItem model and the same CLI rename convention
// ("{imdb-...}" tagging), so the same staleness pattern occurs on either.
func TestDeleteGhostEntries_Case1DeletesRawEntryForNzbProtocol(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	renamed := &storage.Entry{
		Protocol: config.ProtocolNZB, InfoHash: "hash-renamed",
		Name: "Show (2020) - S01E01 - {imdb-tt1234567} - 1080p", OriginalFilename: "raw.release.name.mkv",
	}
	if err := store.AddOrUpdate(renamed); err != nil {
		t.Fatal(err)
	}
	raw := &storage.Entry{
		Protocol: config.ProtocolNZB, InfoHash: "hash-raw",
		Name: "raw.release.name.mkv", OriginalFilename: "raw.release.name.mkv",
	}
	if err := store.AddOrUpdate(raw); err != nil {
		t.Fatal(err)
	}

	// A shared EntryItem folder whose files span both hashes — the scenario
	// Case 1 looks for.
	if err := store.UpdateItem(&storage.EntryItem{
		Name: "shared-folder",
		Files: map[string]*storage.File{
			"file-a.mkv": {Name: "file-a.mkv", InfoHash: "hash-renamed"},
			"file-b.mkv": {Name: "file-b.mkv", InfoHash: "hash-raw"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	mgr := newTestManager(store)
	mgr.deleteGhostEntries()

	if _, err := store.Get("hash-raw"); err == nil {
		t.Fatal("raw duplicate nzb Entry should have been deleted as a ghost of the renamed one")
	}
	if _, err := store.Get("hash-renamed"); err != nil {
		t.Fatalf("renamed nzb Entry must survive: %v", err)
	}
}
