package stats

import (
	"testing"

	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestCombinedContentStats(t *testing.T) {
	debrids := []debridTypes.Stats{
		{Library: debridTypes.LibraryStats{Total: 10, TotalSize: 1000}},
		{Library: debridTypes.LibraryStats{Total: 20, TotalSize: 2000}},
	}
	usenet := map[string]any{
		"content_count": 5,
		"content_size":  int64(500),
	}

	got := combinedContentStats(debrids, usenet)
	if got.TotalCount != 35 || got.TotalSize != 3500 {
		t.Fatalf("combinedContentStats() = %+v, want TotalCount=35 TotalSize=3500", got)
	}
}

func TestCombinedContentStatsNoUsenet(t *testing.T) {
	debrids := []debridTypes.Stats{
		{Library: debridTypes.LibraryStats{Total: 10, TotalSize: 1000}},
	}

	got := combinedContentStats(debrids, nil)
	if got.TotalCount != 10 || got.TotalSize != 1000 {
		t.Fatalf("combinedContentStats() = %+v, want TotalCount=10 TotalSize=1000", got)
	}
}

func TestCombinedContentStatsNoDebrids(t *testing.T) {
	usenet := map[string]any{
		"content_count": 7,
		"content_size":  int64(700),
	}

	got := combinedContentStats(nil, usenet)
	if got.TotalCount != 7 || got.TotalSize != 700 {
		t.Fatalf("combinedContentStats() = %+v, want TotalCount=7 TotalSize=700", got)
	}
}

// Malformed/unexpected value types in the usenet map must not panic or
// silently corrupt the total — the type assertions in combinedContentStats
// are the only thing standing between Manager.UsenetStats()'s untyped
// map[string]any and a bad cast.
func TestCombinedContentStatsMalformedUsenetValues(t *testing.T) {
	usenet := map[string]any{
		"content_count": "not-an-int",
		"content_size":  "not-an-int64",
	}

	got := combinedContentStats(nil, usenet)
	if got.TotalCount != 0 || got.TotalSize != 0 {
		t.Fatalf("combinedContentStats() = %+v, want zero value on type mismatch", got)
	}
}
