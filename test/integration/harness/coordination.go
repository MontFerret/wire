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

func NewBlock(t testing.TB) *Block {
	block := &Block{Started: make(chan struct{}), Cancelled: make(chan struct{}), Finished: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(block.Release)

	return block
}

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

func (b *Block) Release() {
	b.once.Do(func() { close(b.release) })
}

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

func Context(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	return ctx
}
