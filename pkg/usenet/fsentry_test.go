package usenet

import (
	"sync"
	"sync/atomic"
	"testing"
)

// acquire/claimForCleanup close the race between a stream picking up an
// fsEntry from the map and cleanupIdleFS tearing that same entry down: once
// claimForCleanup wins, no later acquire can succeed, so cleanup never runs
// concurrently with a stream that thinks it holds a live reference.

func TestFsEntryAcquireSucceedsWhileActive(t *testing.T) {
	fe := &fsEntry{}
	if !fe.acquire() {
		t.Fatal("acquire() on a fresh entry should succeed")
	}
	if got := fe.refCount.Load(); got != 1 {
		t.Fatalf("refCount = %d, want 1", got)
	}
}

func TestFsEntryClaimForCleanupRequiresZeroRefCount(t *testing.T) {
	fe := &fsEntry{}
	fe.acquire() // refCount = 1

	if fe.claimForCleanup() {
		t.Fatal("claimForCleanup should fail while refCount > 0")
	}
	if got := fe.refCount.Load(); got != 1 {
		t.Fatalf("refCount = %d, want unchanged 1", got)
	}
}

func TestFsEntryAcquireFailsAfterClaim(t *testing.T) {
	fe := &fsEntry{} // refCount = 0, idle

	if !fe.claimForCleanup() {
		t.Fatal("claimForCleanup should succeed on an idle entry")
	}
	if fe.acquire() {
		t.Fatal("acquire() must fail once the entry is claimed for cleanup")
	}
	// The tombstone must not look like a normal positive refCount to any
	// caller that (incorrectly) reads it directly.
	if got := fe.refCount.Load(); got >= 0 {
		t.Fatalf("refCount = %d, want negative tombstone", got)
	}
}

// TestFsEntryConcurrentAcquireVsClaim hammers acquire/release against a
// single claimForCleanup from many goroutines. It asserts the invariant
// acquire()/claimForCleanup() exist to guarantee: once the entry is claimed,
// every subsequent acquire() call fails, with no window where a claimed
// entry still hands out references. Run with -race to also catch any data
// race in the CAS loop itself.
func TestFsEntryConcurrentAcquireVsClaim(t *testing.T) {
	fe := &fsEntry{}
	const workers = 50

	var claimed atomic.Bool
	var sawAcquireAfterClaim atomic.Bool
	var wg sync.WaitGroup

	// One claimer, competing with many acquirers releasing immediately (the
	// realistic pattern: a stream acquires, does work, releases).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if fe.refCount.Load() == 0 && fe.claimForCleanup() {
				claimed.Store(true)
				return
			}
		}
	}()

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if !fe.acquire() {
					if claimed.Load() {
						continue // expected once claimed
					}
					continue
				}
				if claimed.Load() {
					// Observing a successful acquire after the claimer has
					// already won is the exact race this mechanism prevents.
					sawAcquireAfterClaim.Store(true)
				}
				fe.refCount.Add(-1)
			}
		}()
	}

	wg.Wait()

	if !claimed.Load() {
		t.Fatal("claimer never won — test did not exercise the race")
	}
	if sawAcquireAfterClaim.Load() {
		t.Fatal("acquire() succeeded after claimForCleanup had already claimed the entry")
	}
}
