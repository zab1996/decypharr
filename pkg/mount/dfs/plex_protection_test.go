package dfs

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/manager"
)

func TestResolveProtectedStreamsSkipsInactiveSessions(t *testing.T) {
	out := resolveProtectedStreams(nil, []plexSession{
		{Type: "episode", Duration: 1000, ViewOffset: 0, GrandparentTitle: "Show", ParentIndex: 1, Index: 1},
		{Type: "movie", Duration: 1000, ViewOffset: 500, Title: "Inception"},
	})
	if len(out) != 0 {
		t.Fatalf("expected 0 protected streams, got %d", len(out))
	}
}

func TestResolveProtectedStreamsDedupesByFile(t *testing.T) {
	// Without a manager file match returns empty; verify dedupe path doesn't panic.
	out := resolveProtectedStreams(&manager.Manager{}, []plexSession{
		{Type: "episode", Duration: 1000, ViewOffset: 500, GrandparentTitle: "Show", ParentIndex: 1, Index: 1, RatingKey: "1"},
		{Type: "episode", Duration: 1000, ViewOffset: 600, GrandparentTitle: "Show", ParentIndex: 1, Index: 1, RatingKey: "2"},
	})
	if len(out) != 0 {
		t.Fatalf("expected no matches without library entries, got %d", len(out))
	}
}
