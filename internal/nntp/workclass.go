package nntp

import "context"

type workClassKey struct{}

// WorkClass tags NNTP pool acquisitions as stream (playback) or background work.
type WorkClass int

const (
	WorkClassBackground WorkClass = iota
	WorkClassStream
)

// WithWorkClass returns a child context tagged with the given work class.
func WithWorkClass(ctx context.Context, class WorkClass) context.Context {
	return context.WithValue(ctx, workClassKey{}, class)
}

// WorkClassFromContext returns the work class stored on ctx, defaulting to background.
func WorkClassFromContext(ctx context.Context) WorkClass {
	if ctx == nil {
		return WorkClassBackground
	}
	if v, ok := ctx.Value(workClassKey{}).(WorkClass); ok {
		return v
	}
	return WorkClassBackground
}
