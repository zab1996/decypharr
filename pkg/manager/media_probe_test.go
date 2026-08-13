package manager

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

func TestClassifyMountedReadErrorIsUnknown(t *testing.T) {
	got := classifyMountedReadError(syscall.EIO)
	if got.state != mediaProbeUnknown || got.reason != "media_probe_unavailable" {
		t.Fatalf("EIO classification = %#v, want unknown/media_probe_unavailable", got)
	}
}

func TestClassifyFFProbeOutput(t *testing.T) {
	tests := []struct {
		name   string
		json   string
		state  mediaProbeState
		reason string
	}{
		{"video", `{"streams":[{"codec_type":"video"}]}`, mediaProbeHealthy, ""},
		{"audio", `{"streams":[{"codec_type":"audio"}]}`, mediaProbeHealthy, ""},
		{"subtitle only", `{"streams":[{"codec_type":"subtitle"}]}`, mediaProbeBroken, "media_no_playable_stream"},
		{"invalid output", `{`, mediaProbeUnknown, "media_probe_unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyFFProbeOutput([]byte(tt.json))
			if got.state != tt.state || got.reason != tt.reason {
				t.Fatalf("classification = %#v, want state=%v reason=%q", got, tt.state, tt.reason)
			}
		})
	}
}

func TestMountedMediaProbeRetriesOnlyBroken(t *testing.T) {
	attempts := 0
	repair := &Repair{
		mediaProbeSlots: make(chan struct{}, 4),
		mediaProbeAttempt: func(context.Context, string) mediaProbeResult {
			attempts++
			if attempts == 1 {
				return mediaProbeResult{state: mediaProbeBroken, reason: "media_probe_failed"}
			}
			return mediaProbeResult{state: mediaProbeHealthy}
		},
	}
	if got := repair.probeMountedMedia(context.Background(), "unused"); got.state != mediaProbeHealthy || attempts != 2 {
		t.Fatalf("probe = %#v after %d attempts, want healthy after retry", got, attempts)
	}

	attempts = 0
	repair.mediaProbeAttempt = func(context.Context, string) mediaProbeResult {
		attempts++
		return classifyMountedReadError(errors.New("temporary mount failure"))
	}
	if got := repair.probeMountedMedia(context.Background(), "unused"); got.state != mediaProbeUnknown || attempts != 1 {
		t.Fatalf("unknown probe = %#v after %d attempts, want one attempt", got, attempts)
	}
}

func TestMountedMediaPathRejectsTraversal(t *testing.T) {
	if _, ok := mountedMediaPath("/mnt", "entry", "../other/file.mkv"); ok {
		t.Fatal("path traversal unexpectedly accepted")
	}
	if got, ok := mountedMediaPath("/mnt", "entry", "episode.mkv"); !ok || got != "/mnt/__all__/entry/episode.mkv" {
		t.Fatalf("mounted path = %q, %v", got, ok)
	}
}
