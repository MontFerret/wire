package lifecycle

import (
	"context"
	"errors"
)

var errReleasePanicked = errors.New("resource release panicked")

// Handle coordinates the open, closing, and closed states of one resource.
// It owns synchronization only; the resource supplies its release operation.
type Handle struct {
	close Close
}

// Open reports whether release has not yet been committed.
func (h *Handle) Open() bool {
	return h != nil && !h.close.Started()
}

// Close commits release once and lets each caller wait with its own context.
// Release does not inherit caller cancellation, but retains its deadline.
func (h *Handle) Close(ctx context.Context, release func(context.Context) error) error {
	if h.close.Begin() {
		retained, cancel := retainedContext(ctx)

		go h.settle(retained, cancel, release)
	}

	return h.close.Wait(ctx)
}

// CloseResult reports whether release has started and, when it has, waits for
// its retained result with ctx.
func (h *Handle) CloseResult(ctx context.Context) (bool, error) {
	if h == nil || !h.close.Started() {
		return false, nil
	}

	return true, h.close.Wait(ctx)
}

func (h *Handle) settle(ctx context.Context, cancel context.CancelFunc, release func(context.Context) error) {
	defer cancel()

	var result error
	defer func() {
		if recover() != nil {
			result = errors.Join(result, errReleasePanicked)
		}

		h.close.Finish(result)
	}()

	result = release(ctx)
}

func retainedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.Background(), func() {}
	}

	return context.WithDeadline(context.Background(), deadline)
}
