package server_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestRuntimeAllocationCancellationBeforeDispatch(t *testing.T) {
	for _, operation := range allocationOperations() {
		for _, stage := range []string{"before call", "option callback"} {
			if operation.name == "session run" && stage == "option callback" {
				continue
			}

			t.Run(operation.name+"/"+stage, func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				before := f.gate.count(operation.method)
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()
				var optionCancel context.CancelFunc
				if stage == "before call" {
					cancel()
				} else {
					optionCancel = cancel
				}

				closeHandle, err := f.allocate(ctx, optionCancel)
				if !errors.Is(err, context.Canceled) || closeHandle != nil {
					t.Fatalf("cancelled acquisition returned a handle or lost cancellation: %v", err)
				}

				if calls := f.gate.count(operation.method); calls != before {
					t.Fatalf("cancelled acquisition sent %d RPCs", calls-before)
				}
			})
		}
	}
}

func TestRuntimeAllocationCancellationAfterCommitReleasesOnlyNewResource(t *testing.T) {
	for _, operation := range allocationOperations() {
		t.Run(operation.name, func(t *testing.T) {
			f := newRuntimeAllocationFixture(t, operation)
			f.gate.arm(operation.method, "success")
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			result := make(chan error, 1)
			go func() {
				closeHandle, err := f.allocate(ctx, nil)
				if closeHandle != nil {
					err = errors.Join(err, errors.New("cancelled allocation returned a handle"), closeHandle())
				}
				result <- err
			}()
			f.awaitCommitted()
			cancel()
			close(f.gate.deliver)
			if err := f.awaitResult(result); !errors.Is(err, context.Canceled) {
				t.Fatalf("caller cancellation was lost: %v", err)
			}

			if calls := f.gate.count(operation.release); calls != 1 {
				t.Fatalf("resource release calls = %d, want 1", calls)
			}

			if calls := f.gate.count(operation.parentRelease); calls != 0 {
				t.Fatalf("healthy parent was released %d times", calls)
			}

			if operation.name == "session run" {
				if _, err := f.session.Run(testContext(t)); err != nil {
					t.Fatalf("durable Session was not reusable after cancellation: %v", err)
				}
			}
		})
	}
}

func TestRuntimeAllocationNormalCloseReleasesOnce(t *testing.T) {
	for _, operation := range allocationOperations() {
		t.Run(operation.name, func(t *testing.T) {
			f := newRuntimeAllocationFixture(t, operation)
			closeHandle, err := f.allocate(testContext(t), nil)
			if err != nil {
				t.Fatal(err)
			}

			if closeHandle != nil {
				results := make(chan error, 8)
				for range 8 {
					go func() { results <- closeHandle() }()
				}
				for range 8 {
					if err := f.awaitResult(results); err != nil {
						t.Fatal(err)
					}
				}
			}

			if calls := f.gate.count(operation.release); calls != 1 {
				t.Fatalf("resource release calls = %d, want 1", calls)
			}
		})
	}
}

func TestRuntimeSessionCompletionRacesCancellationWithoutDuplicateCleanup(t *testing.T) {
	for range 20 {
		operation := allocationOperations()[4]
		f := newRuntimeAllocationFixture(t, operation)
		started, finish := make(chan struct{}), make(chan struct{})
		f.sessions[0].run = func(context.Context, int) (api.Output, error) {
			close(started)
			<-finish

			return api.Output{}, nil
		}
		ctx, cancel := context.WithCancel(testContext(t))
		result := make(chan error, 1)
		go func() {
			_, err := f.session.Run(ctx)
			result <- err
		}()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("hosted run did not start")
		}

		var race sync.WaitGroup
		race.Add(2)
		go func() { defer race.Done(); cancel() }()
		go func() { defer race.Done(); close(finish) }()
		race.Wait()
		if err := f.awaitResult(result); err != nil && !cancellationError(err) && !errors.Is(err, client.ErrExecutionCancelled) {
			t.Fatalf("unexpected completion race error: %v", err)
		}

		if f.gate.count(operation.release) != 1 || f.gate.count(wirev1.ExecutionService_CancelExecution_FullMethodName) != 0 {
			t.Fatal("adapter did not use exactly one execution release")
		}

		f.sessions[0].mu.Lock()
		f.sessions[0].run = nil
		f.sessions[0].mu.Unlock()
		if _, err := f.session.Run(testContext(t)); err != nil {
			t.Fatalf("Session retained a leaked execution: %v", err)
		}

		if runs, closes := f.sessions[0].counts(); runs != 2 || closes != 0 {
			t.Fatalf("durable Session lifecycle changed: runs=%d closes=%d", runs, closes)
		}
	}
}
