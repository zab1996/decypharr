package manager

import (
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestInteractiveMonitorDetectsSustainedReads(t *testing.T) {
	var active bool
	m := newInteractiveMonitor(&config.Config{
		Usenet: config.Usenet{
			InteractivePoolReserveEnabled: true,
			InteractiveDetectBytes:        "4MB",
			InteractiveDetectWindow:       "5s",
			InteractiveIdleTimeout:        "30s",
		},
	}, func(a bool, _ reserveStreamMeta, _ int64, _ time.Duration) {
		active = a
	})

	meta := reserveStreamMeta{Entry: "Show", File: "ep.mkv", Client: "DFS"}
	m.RecordRead(2<<20, false, meta)
	if active {
		t.Fatal("should not activate below threshold")
	}
	m.RecordRead(3<<20, false, meta)
	if !active {
		t.Fatal("expected interactive mode after sustained reads")
	}
}

func TestInteractiveMonitorIgnoresProbes(t *testing.T) {
	var active bool
	m := newInteractiveMonitor(&config.Config{
		Usenet: config.Usenet{
			InteractivePoolReserveEnabled: true,
			InteractiveDetectBytes:        "1MB",
		},
	}, func(a bool, _ reserveStreamMeta, _ int64, _ time.Duration) {
		active = a
	})
	m.RecordRead(5<<20, true, reserveStreamMeta{Entry: "Show", File: "ep.mkv"})
	if active {
		t.Fatal("probe reads must not activate interactive mode")
	}
}

func TestInteractiveMonitorIdleTimeout(t *testing.T) {
	var active bool
	m := newInteractiveMonitor(&config.Config{
		Usenet: config.Usenet{
			InteractivePoolReserveEnabled: true,
			InteractiveDetectBytes:        "1MB",
			InteractiveIdleTimeout:        "1s",
		},
	}, func(a bool, _ reserveStreamMeta, _ int64, _ time.Duration) {
		active = a
	})
	m.RecordRead(2<<20, false, reserveStreamMeta{Entry: "Show", File: "ep.mkv"})
	if !active {
		t.Fatal("expected active")
	}
	m.Tick(time.Now().Add(2 * time.Second))
	if active {
		t.Fatal("expected idle timeout to deactivate")
	}
}
