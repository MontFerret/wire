// Package lifecycle coordinates retained teardown results and independently cancellable waiters.
package lifecycle

import (
	"context"
	"sync"
)

// Close coordinates one committed teardown and lets callers wait with their
// own contexts without transferring ownership of the teardown.
type Close struct {
	mu      sync.Mutex
	started bool
	done    chan struct{}
	err     error
}

// Begin reports whether the caller owns teardown.
func (c *Close) Begin() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return false
	}

	c.started = true
	c.done = make(chan struct{})

	return true
}

// Started reports whether teardown has been committed.
func (c *Close) Started() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.started
}

// Finish publishes the retained teardown result.
func (c *Close) Finish(err error) {
	c.mu.Lock()
	if !c.started || c.done == nil {
		c.mu.Unlock()
		panic("lifecycle: finish before begin")
	}

	c.err = err
	done := c.done
	c.done = nil
	c.mu.Unlock()

	close(done)
}

// Wait returns the teardown result or the caller's context error.
func (c *Close) Wait(ctx context.Context) error {
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
