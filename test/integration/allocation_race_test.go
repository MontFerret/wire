package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/wire/test/integration/harness"
)

func TestRuntimeAllocationCancellationBeforeDispatch(t *testing.T) {
	for _, operation := range allocationOperations() {
		for _, stage := range []string{"before call", "option callback"} {
			if operation.name == "session run" && stage == "option callback" {
				continue
			}

			t.Run(operation.name+"/"+stage, func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				before := f.gate.Count(operation.method)
				ctx, cancel := context.WithCancel(harness.Context(t))
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

				if calls := f.gate.Count(operation.method); calls != before {
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
			f.reply = f.gate.Arm(operation.method, harness.Deliver)
			ctx, cancel := context.WithCancel(harness.Context(t))
			defer cancel()
			result := make(chan error, 1)
			go func() {
				closeHandle, err := f.allocate(ctx, nil)
				if closeHandle != nil {
					t.Error("cancelled allocation returned a handle")
					err = errors.Join(err, errors.New("cancelled allocation returned a handle"), closeHandle())
				}

				result <- err
			}()
			f.awaitCommitted()
			cancel()
			f.reply.Deliver()

			if err := f.awaitResult(result); !errors.Is(err, context.Canceled) {
				t.Fatalf("caller cancellation was lost: %v", err)
			}

			if calls := f.gate.Count(operation.release); calls != 1 {
				t.Fatalf("resource release calls = %d, want 1", calls)
			}

			if calls := f.gate.Count(operation.parentRelease); calls != 0 {
				t.Fatalf("healthy parent was released %d times", calls)
			}

			if operation.name == "session run" {
				if _, err := f.session.Run(harness.Context(t)); err != nil {
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

			closeHandle, err := f.allocate(harness.Context(t), nil)
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

			if calls := f.gate.Count(operation.release); calls != 1 {
				t.Fatalf("resource release calls = %d, want 1", calls)
			}
		})
	}
}
