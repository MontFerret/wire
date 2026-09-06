package harness

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Block provides observable operation entry, cancellation, and settlement.
// A fixture must use a fresh Block for each independently blocked invocation.
type Block struct {
	Started   chan struct{}
	Cancelled chan struct{}
	Finished  chan struct{}
	release   chan struct{}
	once      sync.Once
}

// NewBlock creates a single-invocation barrier that test cleanup always releases.
func NewBlock(t testing.TB) *Block {
	block := &Block{Started: make(chan struct{}), Cancelled: make(chan struct{}), Finished: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(block.Release)

	return block
}

// Wait announces entry and waits for release or cancellation, then announces settlement.
func (b *Block) Wait(ctx context.Context) error {
	close(b.Started)
	defer close(b.Finished)

	select {
	case <-ctx.Done():
		close(b.Cancelled)

		return ctx.Err()
	case <-b.release:
		return nil
	}
}

// Release unblocks the invocation and is safe to call repeatedly.
func (b *Block) Release() {
	b.once.Do(func() { close(b.release) })
}

// Await receives a coordinated result or fails the test after ten seconds.
func Await[T any](t testing.TB, channel <-chan T) T {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case value := <-channel:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for coordinated operation")

		var zero T

		return zero
	}
}

// Context creates a ten-second operation context cancelled by test cleanup.
func Context(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	return ctx
}
