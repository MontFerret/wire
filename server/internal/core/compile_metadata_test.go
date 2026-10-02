package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MontFerret/api"
)

func TestCompileMetadataCancellationClosesUnpublishedPlan(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, mode := range []string{"canceled", "deadline", "success after cancellation", "private error after cancellation"} {
			name := map[bool]string{false: "normal/", true: "debug/"}[debug] + mode
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()

				if mode == "deadline" {
					ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()
				}

				cleanupErr := errors.New("metadata cleanup")
				hosted := &spyPlan{close: func() error { return cleanupErr }, paramsCall: func(received context.Context) ([]string, error) {
					if received != ctx {
						t.Fatal("Params did not receive the allocation context")
					}

					if mode == "deadline" {
						<-received.Done()
					} else {
						cancel()
					}

					if mode == "success after cancellation" {
						return []string{"partial"}, nil
					}

					if mode == "private error after cancellation" {
						return nil, errors.New("private canceled metadata")
					}

					return []string{"partial"}, received.Err()
				}}
				limits := testLimits().resources()
				limits.Plans = 1
				store := newResourceStore(testContext(t), limits)
				runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) { return hosted, nil }}
				plan, err := CompilePlan(ctx, runtime, store, api.Source{}, debug)
				want := context.Canceled

				if mode == "deadline" {
					want = context.DeadlineExceeded
				}

				if plan != nil || !errors.Is(err, want) || !errors.Is(err, cleanupErr) {
					t.Fatalf("metadata cancellation=%v, %v", plan, err)
				}

				if _, _, count := hosted.snapshot(); count != 1 {
					t.Fatalf("unpublished plan closed %d times", count)
				}

				store.mu.Lock()
				retained, pending := len(store.plans), store.pending[planResource]
				store.mu.Unlock()

				if retained != 0 || pending != 0 {
					t.Fatalf("failed metadata retained plans=%d pending=%d", retained, pending)
				}

				replacement, err := CompilePlan(testContext(t), &spyRuntime{}, store, api.Source{}, debug)
				if err != nil {
					t.Fatalf("failed metadata retained quota: %v", err)
				}

				if err := replacement.Release(testContext(t)); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
