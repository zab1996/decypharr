package vfs

import (
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
)

// TestGetFileEvictsStaleEntryOnClaimedItem reproduces the race fixed in
// getFile (GetFile's implementation): the fast path finds an existing
// fileEntry whose underlying CacheItem has just been claimed by the cache
// janitor for teardown (opens dropped to 0 independently of this fileEntry's
// own refCount — see Cache.cleanupItems/claimForClose). Before the fix, the
// fast path detected the failed Open() and fell through to the slow path,
// but left the stale fileEntry in m.files; GetItem's slow path then created
// a genuinely fresh CacheItem, but the subsequent LoadOrStore in getFile's
// slow path would rediscover the same stale entry (never deleted, since
// ReleaseFile — the only other deleter — hadn't run for whatever handle
// still held it), and retry Open() against the same claimed item forever,
// returning an error instead of the fresh item that was just created.
func TestGetFileEvictsStaleEntryOnClaimedItem(t *testing.T) {
	const parent, name = "entry", "file.mkv"
	key := buildFileKey(parent, name)

	claimedItem := &CacheItem{}
	if !claimedItem.claimForClose() {
		t.Fatal("expected claimForClose to succeed on a fresh, unopened item")
	}
	if claimedItem.Open() {
		t.Fatal("Open() must fail on a claimed item")
	}

	staleEntry := &fileEntry{item: claimedItem}
	staleEntry.refCount.Store(1)

	m := &Manager{
		cache: &Cache{items: xsync.NewMap[string, *CacheItem]()},
		files: xsync.NewMap[string, *fileEntry](),
	}
	m.files.Store(key, staleEntry)
	// GetItem's slow path (invoked by getFile once the fast path fails)
	// creates a fresh CacheItem via c.newItem, which needs cache.mgr/config
	// wiring this test doesn't set up. Pre-seed c.items with the item the
	// slow path should find instead, bypassing newItem's dependencies while
	// still exercising the exact map operations getFile performs.
	freshItem := &CacheItem{info: ItemInfo{Size: 1024}}
	m.cache.items.Store(buildCacheKey(parent, name), freshItem)

	sf, err := m.getFile(parent, name, 1024)
	if err != nil {
		t.Fatalf("getFile returned an error instead of recovering with a fresh item: %v", err)
	}
	if sf == nil {
		t.Fatal("expected a non-nil StreamingFile from the fresh item")
	}

	got, ok := m.files.Load(key)
	if !ok {
		t.Fatal("expected a fileEntry to be stored under key after getFile recovers")
	}
	if got == staleEntry {
		t.Fatal("the stale entry (wrapping the claimed item) must not still be the one stored under key")
	}
	if got.item != freshItem {
		t.Fatalf("expected the stored entry to wrap the fresh item, got a different item")
	}
}

// TestGetFileFastPathHappyCase is the non-racy control: an existing,
// unclaimed entry is reused directly via the fast path, with no eviction and
// no slow-path CacheItem creation.
func TestGetFileFastPathHappyCase(t *testing.T) {
	const parent, name = "entry", "file.mkv"
	key := buildFileKey(parent, name)

	item := &CacheItem{info: ItemInfo{Size: 2048}}
	entry := &fileEntry{item: item}
	entry.refCount.Store(1)

	m := &Manager{
		cache: &Cache{items: xsync.NewMap[string, *CacheItem]()},
		files: xsync.NewMap[string, *fileEntry](),
	}
	m.files.Store(key, entry)

	sf, err := m.getFile(parent, name, 2048)
	if err != nil {
		t.Fatalf("getFile returned an unexpected error: %v", err)
	}
	if sf == nil {
		t.Fatal("expected a non-nil StreamingFile")
	}

	got, ok := m.files.Load(key)
	if !ok || got != entry {
		t.Fatal("the fast path must not replace an existing, unclaimed entry")
	}
	if entry.refCount.Load() != 2 {
		t.Fatalf("expected refCount 2 (1 initial + 1 from this open), got %d", entry.refCount.Load())
	}
}
