package manager

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
)

// initJobQueue backgrounds restoreActiveDownloadJobs precisely so a panic
// while restoring a large or corrupt active-download queue can't take the
// whole process down before Manager construction (and the HTTP server) has
// even finished starting. m.queue is deliberately left nil here to force
// restoreActiveDownloadJobs to panic (ListFilter dereferences q.storage on
// a nil *Queue receiver) and prove the recovery actually catches it.
//
// initJobQueue itself must return immediately regardless of how the
// backgrounded restore turns out. If the panic weren't recovered, it would
// crash the entire test binary rather than fail an assertion — reaching the
// end of this test at all, on a nil queue, is the proof recovery works.
func TestInitJobQueue_RecoversPanicInRestore(t *testing.T) {
	m := &Manager{
		ctx:    context.Background(),
		logger: zerolog.Nop(),
		config: &config.Config{MaxActiveDownloads: 1},
		// queue is intentionally nil.
	}

	returned := make(chan struct{})
	go func() {
		m.initJobQueue()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("initJobQueue did not return promptly — it must not block on restoreActiveDownloadJobs")
	}

	// Give the background restore goroutine a moment to actually run and hit
	// the nil-queue panic.
	time.Sleep(200 * time.Millisecond)
}
