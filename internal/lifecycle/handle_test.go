package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandleRetainsOneReleaseResultForConcurrentCallers(t *testing.T) {
	var handle Handle
	if !handle.Open() {
		t.Fatal("new handle is not open")
	}

	entered := make(chan struct{})
	allow := make(chan struct{})
	want := errors.New("release failed")
	var calls atomic.Int32
	release := func(context.Context) error {
		if calls.Add(1) == 1 {
			close(entered)
		}

		<-allow

		return want
	}

	const callers = 8
	start := make(chan struct{})
	results := make(chan error, callers)
	var callersReady sync.WaitGroup
	callersReady.Add(callers)

	for range callers {
		go func() {
			callersReady.Done()
			<-start
			results <- handle.Close(context.Background(), release)
		}()
	}

	callersReady.Wait()
	close(start)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("release did not start")
	}

	if handle.Open() {
		t.Fatal("handle remained open after release started")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if closing, err := handle.CloseResult(cancelled); !closing || !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight close result = %v, %v", closing, err)
	}

	close(allow)
	for range callers {
		if err := <-results; !errors.Is(err, want) {
			t.Fatalf("concurrent caller did not observe retained result: %v", err)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("release ran %d times", got)
	}

	if closing, err := handle.CloseResult(context.Background()); !closing || !errors.Is(err, want) {
		t.Fatalf("completed close result = %v, %v", closing, err)
	}
}

func TestHandleWaiterCancellationDoesNotCancelRelease(t *testing.T) {
	var handle Handle
	entered := make(chan context.Context, 1)
	allow := make(chan struct{})
	release := func(ctx context.Context) error {
		entered <- ctx
		<-allow

		return nil
	}

	waiter, cancelWaiter := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- handle.Close(waiter, release) }()

	var releaseCtx context.Context
	select {
	case releaseCtx = <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("release did not start")
	}

	cancelWaiter()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter returned %v", err)
	}

	if err := releaseCtx.Err(); err != nil {
		t.Fatalf("waiter cancellation reached committed release: %v", err)
	}

	close(allow)
	if err := handle.Close(context.Background(), release); err != nil {
		t.Fatalf("later waiter did not observe release completion: %v", err)
	}
}

func TestHandlePreservesReleaseDeadline(t *testing.T) {
	var handle Handle
	deadline := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	deadlines := make(chan time.Time, 1)
	if err := handle.Close(ctx, func(releaseCtx context.Context) error {
		got, ok := releaseCtx.Deadline()
		if !ok {
			return errors.New("release context lost its deadline")
		}

		deadlines <- got

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	got := <-deadlines
	if !got.Equal(deadline) {
		t.Fatalf("release deadline = %v, want %v", got, deadline)
	}
}

func TestHandleRetainsSanitizedPanicResult(t *testing.T) {
	var handle Handle
	err := handle.Close(context.Background(), func(context.Context) error {
		panic("sensitive panic value")
	})
	if !errors.Is(err, errReleasePanicked) || err.Error() != errReleasePanicked.Error() {
		t.Fatalf("panic result was not sanitized: %v", err)
	}

	if err := handle.Close(context.Background(), nil); !errors.Is(err, errReleasePanicked) {
		t.Fatalf("panic result was not retained: %v", err)
	}
}

func TestHandleCloseResultLeavesOpenHandleAlone(t *testing.T) {
	var handle Handle
	if closing, err := handle.CloseResult(context.Background()); closing || err != nil {
		t.Fatalf("open close result = %v, %v", closing, err)
	}
}
