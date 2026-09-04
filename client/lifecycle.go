package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const convenienceCleanupTimeout = 30 * time.Second

// closeState coordinates one committed handle teardown while allowing each
// caller to wait with its own context.
type closeState struct {
	mu      sync.Mutex
	started bool
	done    chan struct{}
	err     error
}

func (c *closeState) Begin() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return false
	}

	c.started = true
	c.done = make(chan struct{})

	return true
}

func (c *closeState) Started() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.started
}

func (c *closeState) Finish(err error) {
	c.mu.Lock()
	if !c.started || c.done == nil {
		c.mu.Unlock()
		panic("client: finish close before begin")
	}

	c.err = err
	done := c.done
	c.done = nil
	c.mu.Unlock()

	close(done)
}

func (c *closeState) Wait(ctx context.Context) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()

		return nil
	}

	if c.done == nil {
		err := c.err
		c.mu.Unlock()

		return err
	}

	done := c.done
	c.mu.Unlock()

	select {
	case <-done:
		c.mu.Lock()
		err := c.err
		c.mu.Unlock()

		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func boundedCleanup(ctx context.Context, timeout time.Duration, close func(context.Context) error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	err := close(cleanupCtx)
	if ctxErr := cleanupCtx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		err = errors.Join(err, ctxErr)
	}

	return err
}

func retainedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.Background(), func() {}
	}

	return context.WithDeadline(context.Background(), deadline)
}

func settleHandleClose(ctx context.Context, kind string, state *closeState, release func(context.Context) error) {
	retained, cancel := retainedContext(ctx)
	defer cancel()

	var result error
	defer func() {
		if recover() != nil {
			result = errors.Join(result, fmt.Errorf("Wire %s close panicked", kind))
		}

		state.Finish(result)
	}()

	result = release(retained)
}
