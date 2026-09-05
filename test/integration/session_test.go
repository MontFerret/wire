package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSessionRejectsOverlapAndReopensAfterRelease(t *testing.T) {
	block := harness.NewBlock(t)
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Session: func(harness.SessionOptions) harness.SessionBehavior {
		return harness.SessionBehavior{Run: func(ctx context.Context, call int) (api.Output, error) {
			if call == 1 {
				return api.Output{}, block.Wait(ctx)
			}

			return api.Output{ContentType: "text/plain", Content: []byte("reused")}, nil
		}}
	}}}))
	plan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}

	session, err := plan.NewSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := session.Run(ctx)
		result <- err
	}()
	harness.Await(t, block.Started)
	_, err = session.Run(h.Context())
	var remote *client.Error
	if !errors.As(err, &remote) || remote.Category != failure.CategoryInvalidState || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("overlapping Run=%v", err)
	}

	cancel()

	if err := harness.Await(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation=%v", err)
	}

	harness.Await(t, block.Cancelled)
	harness.Await(t, block.Finished)
	output, err := session.Run(h.Context())
	if err != nil || string(output.Content) != "reused" {
		t.Fatalf("durable session not reusable: %+v %v", output, err)
	}

	snapshot := h.RuntimeSpy().Recorder().Snapshot()
	sessions := snapshot.OfKind("session")
	if len(sessions) != 1 || snapshot.Count(sessions[0].ID, "Run") != 2 || snapshot.Count(sessions[0].ID, "Close") != 0 {
		t.Fatalf("hosted session lifecycle=%+v", snapshot)
	}
}

func TestSessionCompletionRacesCancellationWithoutDuplicateCleanup(t *testing.T) {
	for iteration := range 20 {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			block := harness.NewBlock(t)
			limits := server.DefaultLimits()
			limits.MaxExecutionsPerConnection = 1
			h := harness.New(t, harness.WithServerOptions(server.WithLimits(limits)), harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Session: func(harness.SessionOptions) harness.SessionBehavior {
				return harness.SessionBehavior{Run: func(ctx context.Context, call int) (api.Output, error) {
					if call == 1 {
						return api.Output{}, block.Wait(ctx)
					}

					return api.Output{}, nil
				}}
			}}}))
			plan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 1"})
			if err != nil {
				t.Fatal(err)
			}

			session, err := plan.NewSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(h.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := session.Run(ctx)
				result <- err
			}()
			harness.Await(t, block.Started)
			var race sync.WaitGroup
			race.Add(2)
			go func() {
				defer race.Done()
				cancel()
			}()
			go func() {
				defer race.Done()
				block.Release()
			}()
			race.Wait()

			if err := harness.Await(t, result); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, client.ErrExecutionCancelled) {
				t.Fatalf("completion race=%v", err)
			}

			if h.Faults().Count(harness.ReleaseExecution) != 1 || h.Faults().Count(harness.CancelExecution) != 0 {
				t.Fatal("execution cleanup duplicated")
			}

			if _, err := session.Run(h.Context()); err != nil {
				t.Fatalf("execution capacity leaked: %v", err)
			}

			snapshot := h.RuntimeSpy().Recorder().Snapshot()
			id := snapshot.OfKind("session")[0].ID
			if snapshot.Count(id, "Run") != 2 || snapshot.Count(id, "Close") != 0 {
				t.Fatalf("durable session changed: %+v", snapshot)
			}
		})
	}
}
