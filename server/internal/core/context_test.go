package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// callbackLifetime exposes the standard context AfterFunc contract so the test
// can observe detachment without inspecting context's private implementation.
type callbackLifetime struct {
	context.Context
	done      chan struct{}
	callbacks atomic.Int32
}

func (c *callbackLifetime) Done() <-chan struct{} {
	return c.done
}

func (c *callbackLifetime) AfterFunc(func()) func() bool {
	c.callbacks.Add(1)
	var stopped atomic.Bool

	return func() bool {
		if !stopped.CompareAndSwap(false, true) {
			return false
		}

		c.callbacks.Add(-1)

		return true
	}
}

func TestOperationContextDetachesLifetimeCallback(t *testing.T) {
	lifetime := &callbackLifetime{Context: context.Background(), done: make(chan struct{})}
	operation, cancel := OperationContext(context.Background(), lifetime)
	if lifetime.callbacks.Load() != 1 {
		t.Fatal("operation did not register its lifetime callback")
	}

	cancel()
	cancel()
	if lifetime.callbacks.Load() != 0 || !errors.Is(operation.Err(), context.Canceled) {
		t.Fatal("operation cancellation did not detach its lifetime callback")
	}
}

func TestOperationContextPreservesCancellationCauses(t *testing.T) {
	for _, source := range []string{"request", "lifetime", "already cancelled lifetime", "deadline"} {
		t.Run(source, func(t *testing.T) {
			request, cancelRequest := context.WithCancelCause(context.Background())
			defer cancelRequest(nil)

			lifetime, cancelLifetime := context.WithCancelCause(context.Background())
			defer cancelLifetime(nil)

			cause := errors.New("cancellation cause")
			if source == "already cancelled lifetime" {
				cancelLifetime(cause)
			}

			if source == "deadline" {
				var cancel context.CancelFunc
				request, cancel = context.WithDeadlineCause(request, time.Now().Add(-time.Second), cause)
				defer cancel()
			}

			operation, cancel := OperationContext(request, lifetime)
			defer cancel()

			switch source {
			case "request":
				cancelRequest(cause)
			case "lifetime":
				cancelLifetime(cause)
			}

			select {
			case <-operation.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("operation did not cancel")
			}

			if !errors.Is(context.Cause(operation), cause) {
				t.Fatalf("cancellation cause = %v, want %v", context.Cause(operation), cause)
			}

			want := context.Canceled
			if source == "deadline" {
				want = context.DeadlineExceeded
			}

			if !errors.Is(operation.Err(), want) {
				t.Fatalf("operation error = %v, want %v", operation.Err(), want)
			}
		})
	}
}
