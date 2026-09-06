package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestRecursiveCloseReclaimsActiveDescendants(t *testing.T) {
	for _, owner := range []string{"session", "plan", "runtime"} {
		t.Run(owner, func(t *testing.T) {
			normal, debug, direct := harness.NewBlock(t), harness.NewBlock(t), harness.NewBlock(t)
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{
				Run: func(ctx context.Context, src api.Source, _ harness.SessionOptions) (api.Output, error) {
					if src.Content == "blocked" {
						return api.Output{}, direct.Wait(ctx)
					}

					return api.Output{}, nil
				},
				Plan: harness.PlanBehavior{
					Session: func(options harness.SessionOptions) harness.SessionBehavior {
						return harness.SessionBehavior{Run: func(ctx context.Context, _ int) (api.Output, error) {
							if options.Params["block"] == true {
								return api.Output{}, normal.Wait(ctx)
							}

							return api.Output{}, nil
						}}
					},
					Debugger: harness.DebuggerBehavior{Command: func(ctx context.Context, method string, _ int) (*debugger.Event, error) {
						if method == "Continue" {
							return nil, debug.Wait(ctx)
						}

						return &debugger.Event{Reason: debugger.ReasonEntry}, nil
					}},
				},
			}))

			other, err := h.OpenRuntime()
			if err != nil {
				t.Fatal(err)
			}

			plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
			if err != nil {
				t.Fatal(err)
			}

			siblingPlan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 2"})
			if err != nil {
				t.Fatal(err)
			}

			session, err := plan.NewSession(h.Context(), api.WithParam("block", true))
			if err != nil {
				t.Fatal(err)
			}

			sibling, err := siblingPlan.NewSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			debugSession, err := plan.NewDebugSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			if _, err := debugSession.Start(h.Context()); err != nil {
				t.Fatal(err)
			}

			normalResult := make(chan error, 1)
			go func() {
				_, err := session.Run(h.Context())
				normalResult <- err
			}()
			harness.Await(t, normal.Started)
			type debugOutcome struct {
				event *debugger.Event
				err   error
			}
			debugResult := make(chan debugOutcome, 1)
			directResult := make(chan error, 1)

			if owner != "session" {
				go func() {
					event, err := debugSession.Continue(h.Context())
					debugResult <- debugOutcome{event: event, err: err}
				}()
				harness.Await(t, debug.Started)
			}

			if owner == "runtime" {
				go func() {
					_, err := h.Runtime().Run(h.Context(), api.Source{Content: "blocked"})
					directResult <- err
				}()
				harness.Await(t, direct.Started)
			}

			closeOwner := session.Close

			switch owner {
			case "plan":
				closeOwner = plan.Close
			case "runtime":
				closeOwner = h.Runtime().Close
			}

			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}

			if err := harness.Await(t, normalResult); err == nil {
				t.Fatal("active Run succeeded after recursive close")
			}

			harness.Await(t, normal.Cancelled)
			harness.Await(t, normal.Finished)

			if owner != "session" {
				// A terminated debugger event or a closed-handle error both settle the command.
				result := harness.Await(t, debugResult)
				if result.err == nil && (result.event == nil || result.event.Reason != debugger.ReasonTerminated) {
					t.Fatalf("recursive close returned a successful debugger stop: %#v", result.event)
				}

				harness.Await(t, debug.Cancelled)
				harness.Await(t, debug.Finished)
			} else {
				if _, err := debugSession.Frames(); err != nil {
					t.Fatalf("Session.Close invalidated sibling debugger: %v", err)
				}
			}

			if owner == "runtime" {
				if err := harness.Await(t, directResult); err == nil {
					t.Fatal("direct Run survived Runtime.Close")
				}

				harness.Await(t, direct.Cancelled)
				harness.Await(t, direct.Finished)
			} else {
				if _, err := sibling.Run(h.Context()); err != nil {
					t.Fatalf("unrelated sibling invalidated: %v", err)
				}
			}

			if _, err := other.Run(h.Context(), api.Source{Content: "RETURN 3"}); err != nil {
				t.Fatalf("other logical Runtime invalidated: %v", err)
			}

			snapshot := h.RuntimeSpy().Recorder().Snapshot()

			for _, resource := range snapshot.Resources {
				want := 0

				if resource.Kind == "session" && resource.ID == snapshot.OfKind("session")[0].ID {
					want = 1
				}

				if owner == "plan" && (resource.ID == snapshot.OfKind("plan")[0].ID || resource.Kind == "debugger") {
					want = 1
				}

				if owner == "runtime" && resource.Kind != "runtime" {
					want = 1
				}

				if got := snapshot.Count(resource.ID, "Close"); got != want {
					t.Fatalf("%s close: resource=%+v calls=%d want=%d", owner, resource, got, want)
				}
			}
		})
	}
}

func TestConcurrentSiblingSessionsRemainIndependent(t *testing.T) {
	blocks := []*harness.Block{harness.NewBlock(t), harness.NewBlock(t)}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Session: func(options harness.SessionOptions) harness.SessionBehavior {
		index := int(options.Params["index"].(int64))

		return harness.SessionBehavior{Run: func(ctx context.Context, _ int) (api.Output, error) {
			return api.Output{ContentType: "text/plain", Content: []byte(fmt.Sprint(index))}, blocks[index].Wait(ctx)
		}}
	}}}))

	plan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN @index"})
	if err != nil {
		t.Fatal(err)
	}

	var sessions []api.Session

	for index := range 2 {
		session, err := plan.NewSession(h.Context(), api.WithParam("index", int64(index)))
		if err != nil {
			t.Fatal(err)
		}

		sessions = append(sessions, session)
	}

	type result struct {
		output api.Output
		err    error
	}
	results := []chan result{make(chan result, 1), make(chan result, 1)}

	for index := range 2 {
		go func() {
			output, err := sessions[index].Run(h.Context())
			results[index] <- result{output, err}
		}()
	}

	for _, block := range blocks {
		harness.Await(t, block.Started)
	}

	if err := sessions[0].Close(); err != nil {
		t.Fatal(err)
	}

	if first := harness.Await(t, results[0]); first.err == nil {
		t.Fatal("closed sibling Run succeeded")
	}

	harness.Await(t, blocks[0].Cancelled)
	blocks[1].Release()

	second := harness.Await(t, results[1])
	if second.err != nil || string(second.output.Content) != "1" {
		t.Fatalf("unrelated active sibling affected: %+v", second)
	}

	snapshot := h.RuntimeSpy().Recorder().Snapshot()
	if len(snapshot.OfKind("plan")) != 1 || len(snapshot.OfKind("session")) != 2 {
		t.Fatalf("concurrent sessions changed identity: %+v", snapshot)
	}
}

func TestConcurrentPlanSessionCreation(t *testing.T) {
	const workers = 8
	entered := make(chan struct{}, workers)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{NewSession: func(ctx context.Context, _ harness.SessionOptions) error {
		entered <- struct{}{}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}}}))

	plan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, workers)

	for range workers {
		go func() {
			session, err := plan.NewSession(h.Context())
			if err == nil {
				_, err = session.Run(h.Context())
			}

			results <- err
		}()
	}

	for range workers {
		harness.Await(t, entered)
	}

	finish()

	for range workers {
		if err := harness.Await(t, results); err != nil {
			t.Fatal(err)
		}
	}

	snapshot := h.RuntimeSpy().Recorder().Snapshot()
	if len(snapshot.OfKind("plan")) != 1 || len(snapshot.OfKind("session")) != workers {
		t.Fatalf("concurrent plan creation=%+v", snapshot)
	}
}

func TestConcurrentPlansShareRuntime(t *testing.T) {
	const workers = 4
	entered := make(chan struct{}, workers)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	t.Cleanup(finish)
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Compile: func(ctx context.Context, _ api.Source, _ bool, _ harness.CompileOptions) error {
		entered <- struct{}{}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}}))
	results := make(chan error, workers)

	for index := range workers {
		go func() {
			plan, err := h.Runtime().Compile(h.Context(), api.Source{Name: fmt.Sprintf("plan-%d.fql", index), Content: "RETURN 1"})
			if err != nil {
				results <- err

				return
			}

			session, err := plan.NewSession(h.Context())
			if err == nil {
				_, err = session.Run(h.Context())
			}

			results <- err
		}()
	}

	for range workers {
		harness.Await(t, entered)
	}

	finish()

	for range workers {
		if err := harness.Await(t, results); err != nil {
			t.Fatal(err)
		}
	}

	snapshot := h.RuntimeSpy().Recorder().Snapshot()
	if len(snapshot.OfKind("plan")) != workers || len(snapshot.OfKind("session")) != workers || snapshot.Count(h.RuntimeSpy().ID(), "Compile") != workers {
		t.Fatalf("concurrent plans changed identity: %+v", snapshot)
	}

	parents := make(map[int]bool)

	for _, session := range snapshot.OfKind("session") {
		parents[session.Parent] = true
	}

	if len(parents) != workers {
		t.Fatal("sessions from distinct compiled plans shared a hosted Plan")
	}
}
