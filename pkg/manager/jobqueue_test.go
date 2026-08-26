package manager

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A panic inside processFunc used to unwind the worker goroutine itself —
// with a fixed-size pool, enough panicking jobs silently drained it to zero,
// leaving every subsequently queued job stuck forever with no error and a
// passing healthcheck. runJob's recover must keep the worker alive so later
// jobs still get processed.
func TestWorkerSurvivesPanicInProcessFunc(t *testing.T) {
	var (
		mu        sync.Mutex
		processed []string
	)

	processFunc := func(_ context.Context, job *Job) {
		if job.ID == "panics" {
			panic("boom")
		}
		mu.Lock()
		processed = append(processed, job.ID)
		mu.Unlock()
	}

	q := NewJobQueue(context.Background(), 1, processFunc)
	defer q.Close()

	if err := q.Submit(&Job{ID: "panics"}); err != nil {
		t.Fatalf("Submit(panics) error = %v", err)
	}
	if err := q.Submit(&Job{ID: "after"}); err != nil {
		t.Fatalf("Submit(after) error = %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		got := len(processed)
		mu.Unlock()
		if got >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("worker never processed the job submitted after the panic")
		case <-time.After(10 * time.Millisecond):
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 1 || processed[0] != "after" {
		t.Fatalf("processed = %v, want [after]", processed)
	}
}

// runJob itself must not propagate the panic to its caller.
func TestRunJobRecoversPanic(t *testing.T) {
	var ran atomic.Bool
	q := &JobQueue{
		logger: zerolog.Nop(),
		processFunc: func(_ context.Context, _ *Job) {
			ran.Store(true)
			panic("boom")
		},
	}

	q.runJob(&Job{ID: "x"})

	if !ran.Load() {
		t.Fatal("processFunc was never invoked")
	}
}
