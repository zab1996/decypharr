package vfs

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
)

// Manager manages VFS lifecycle
type Manager struct {
	manager *manager.Manager
	cache   *Cache
	logger  zerolog.Logger

	files *xsync.Map[string, *fileEntry]

	ctx    context.Context
	cancel context.CancelFunc

	totalFiles  atomic.Int32
	activeFiles atomic.Int32
}

// Manager returns the underlying top-level manager, for backends that need
// to report back to it directly (e.g. live read-failure health reporting).
func (m *Manager) Manager() *manager.Manager {
	return m.manager
}

// fileEntry tracks file metadata
type fileEntry struct {
	item     *CacheItem
	refCount atomic.Int32
	// deleted is set to true by ReleaseFile before the entry is removed from the
	// map. GetFile checks this after incrementing refCount so it can detect and
	// undo a concurrent deletion without holding a coarse lock.
	deleted atomic.Bool
}

// NewManager creates a new VFS manager
func NewManager(ctx context.Context, mgr *manager.Manager, config *config.FuseConfig) (*Manager, error) {
	ctx, cancel := context.WithCancel(ctx)

	cache, err := NewCache(ctx, mgr, config)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	m := &Manager{
		manager: mgr,
		cache:   cache,
		logger:  logger.New("vfs"),
		files:   xsync.NewMap[string, *fileEntry](),
		ctx:     ctx,
		cancel:  cancel,
	}

	return m, nil
}

func (m *Manager) GetManager() *manager.Manager {
	return m.manager
}

// GetFile returns a streaming file handle
func (m *Manager) GetFile(info *manager.FileInfo) (*StreamingFile, error) {
	return m.getFile(info.Parent(), info.Name(), info.Size())
}

// getFile is GetFile's implementation, taking the cache key's raw components
// instead of *manager.FileInfo so the fast-path/slow-path/eviction logic is
// testable directly without needing a real manager.Manager to construct a
// FileInfo (its fields are unexported outside pkg/manager).
func (m *Manager) getFile(parent, name string, size int64) (*StreamingFile, error) {
	key := buildFileKey(parent, name)

	// Fast path: existing file.
	// Increment refCount first, then verify the entry wasn't concurrently deleted
	// by ReleaseFile between our Load and the Add. If it was, undo the increment
	// and fall through to the slow path which will create a fresh entry.
	if entry, ok := m.files.Load(key); ok {
		entry.refCount.Add(1)
		if !entry.deleted.Load() {
			sf, err := NewStreamingFile(entry.item)
			if err == nil {
				return sf, nil
			}
			// Item was claimed for teardown between our Load and Open. The
			// underlying CacheItem is being replaced (see Cache.GetItem), but
			// this fileEntry itself is not: it stays in m.files, still
			// pointing at the claimed item, until ReleaseFile eventually runs
			// for whichever handle is still holding it open. If we simply
			// fell through, the slow path's LoadOrStore below would find this
			// same stale entry and retry against the same claimed item
			// forever (EIO for every GetFile on this path until that
			// unrelated ReleaseFile call happens to land). Evict the stale
			// entry now — conditioned on it still being the exact entry we
			// failed against, so we don't clobber one a concurrent slow path
			// already replaced it with — so the slow path below creates and
			// stores a genuinely fresh entry instead.
			m.files.Compute(key, func(oldValue *fileEntry, loaded bool) (*fileEntry, xsync.ComputeOp) {
				if loaded && oldValue == entry {
					return nil, xsync.DeleteOp
				}
				return oldValue, xsync.CancelOp
			})
		}
		entry.refCount.Add(-1)
	}

	// Get or create cache item
	item, err := m.cache.GetItem(parent, name, size)
	if err != nil {
		return nil, fmt.Errorf("failed to get cache item: %w", err)
	}

	entry := &fileEntry{item: item}
	entry.refCount.Store(1)

	// Store or return existing
	actual, loaded := m.files.LoadOrStore(key, entry)
	if loaded {
		// Another goroutine created it first
		actual.refCount.Add(1)
		sf, err := NewStreamingFile(actual.item)
		if err != nil {
			actual.refCount.Add(-1)
			return nil, fmt.Errorf("failed to open cache item: %w", err)
		}
		return sf, nil
	}

	m.totalFiles.Add(1)
	m.activeFiles.Add(1)
	sf, err := NewStreamingFile(item)
	if err != nil {
		// item was just created by us via GetItem above and stored in
		// entry — it cannot already be claimed. Treat as unexpected.
		m.totalFiles.Add(-1)
		m.activeFiles.Add(-1)
		return nil, fmt.Errorf("failed to open cache item: %w", err)
	}
	return sf, nil
}

// ReleaseFile decrements the reference count
func (m *Manager) ReleaseFile(info *manager.FileInfo) {
	key := buildFileKey(info.Parent(), info.Name())

	if entry, ok := m.files.Load(key); ok {
		if entry.refCount.Add(-1) <= 0 {
			// Mark deleted before removing from the map so that any concurrent
			// GetFile that already loaded this entry can detect the deletion and
			// undo its refCount increment rather than using a stale entry.
			entry.deleted.Store(true)
			m.files.Delete(key)
			m.activeFiles.Add(-1)
			// Downloaders are stopped in CacheItem.Release() when opens reaches 0.
		}
	}
}

// Close shuts down the manager
func (m *Manager) Close() error {
	m.cancel()

	// Close all files
	m.files.Range(func(key string, entry *fileEntry) bool {
		if entry.item != nil {
			entry.item.Close()
		}
		return true
	})
	m.files.Clear()

	// Close cache
	if m.cache != nil {
		m.cache.Close()
	}

	return nil
}

// GetStats returns manager statistics
func (m *Manager) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"type":         "dfs",
		"ready":        true,
		"enabled":      true,
		"total_files":  m.totalFiles.Load(),
		"active_files": m.activeFiles.Load(),
	}

	// Add cache stats
	if m.cache != nil {
		for k, v := range m.cache.GetStats() {
			stats["cache_"+k] = v
		}
	}

	return stats
}

func (m *Manager) CleanupCache() map[string]interface{} {
	if m.cache == nil {
		return map[string]interface{}{
			"cleanup_status": "unsupported",
			"cleanup_result": "cache is not initialized",
		}
	}
	return m.cache.RunCleanup()
}

func (m *Manager) PurgeCache() map[string]interface{} {
	if m.cache == nil {
		return map[string]interface{}{
			"purge_status": "unsupported",
			"purge_result": "cache is not initialized",
		}
	}
	return m.cache.PurgeCache()
}

func buildFileKey(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
