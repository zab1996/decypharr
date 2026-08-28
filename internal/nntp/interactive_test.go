package nntp

import (
	"context"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
)

func TestInteractiveReserveComputation(t *testing.T) {
	state := newInteractiveState(&config.Config{
		Usenet: config.Usenet{
			InteractivePoolReserveEnabled: true,
			InteractivePoolReservePercent: 15,
			InteractivePoolReserveMin:     6,
			InteractivePoolReserveMax:     40,
		},
	})
	state.applyConfig(&config.Config{
		Usenet: config.Usenet{
			InteractivePoolReserveEnabled: true,
			InteractivePoolReservePercent: 15,
			InteractivePoolReserveMin:     6,
			InteractivePoolReserveMax:     40,
		},
	})

	entered, _, snap := state.setActive(true, 310, reserveMeta{Entry: "show", File: "ep.mkv", Client: "DFS"})
	if !entered {
		t.Fatal("expected mode enter")
	}
	if snap.Reserved != 40 {
		t.Fatalf("reserved = %d, want 40", snap.Reserved)
	}
	if snap.BackgroundBudget != 270 {
		t.Fatalf("background budget = %d, want 270", snap.BackgroundBudget)
	}

	_, exited, _ := state.setActive(false, 310, reserveMeta{})
	if !exited {
		t.Fatal("expected mode exit")
	}
}

func TestBackgroundBudgetBlocksWhenFull(t *testing.T) {
	state := newInteractiveState(&config.Config{
		Usenet: config.Usenet{InteractivePoolReserveEnabled: true},
	})
	state.setActive(true, 20, reserveMeta{})

	if !state.acquireBackgroundSlot() {
		t.Fatal("first background slot should succeed")
	}
	for i := 0; i < 13; i++ {
		if !state.acquireBackgroundSlot() {
			t.Fatalf("background slot %d should succeed within budget", i+2)
		}
	}
	if state.acquireBackgroundSlot() {
		t.Fatal("background slot should be blocked when budget exhausted")
	}
	state.releaseBackgroundSlot()
	if !state.acquireBackgroundSlot() {
		t.Fatal("background slot should succeed after release")
	}
}

func TestWorkClassFromContext(t *testing.T) {
	ctx := WithWorkClass(context.Background(), WorkClassStream)
	if got := WorkClassFromContext(ctx); got != WorkClassStream {
		t.Fatalf("WorkClassFromContext = %v, want stream", got)
	}
	if got := WorkClassFromContext(context.Background()); got != WorkClassBackground {
		t.Fatalf("default WorkClassFromContext = %v, want background", got)
	}
}

func TestRepairPoolInteractiveCap(t *testing.T) {
	p := &RepairPool{workers: 20}
	p.setInteractiveCap(8)
	if got := p.effectiveCap(); got != 8 {
		t.Fatalf("effectiveCap = %d, want 8", got)
	}
	p.setInteractiveCap(0)
	if got := p.effectiveCap(); got != 20 {
		t.Fatalf("effectiveCap = %d, want 20 when cap cleared", got)
	}
}
