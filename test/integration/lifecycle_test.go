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

func TestParentClosePreservesChildrenAndActiveWork(t *testing.T) {
	for _, owner := range []string{"plan", "runtime"} {
		t.Run(owner, func(t *testing.T) {
			run := harness.NewBlock(t)
			command := harness.NewBlock(t)
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{
				Session: func(harness.SessionOptions) harness.SessionBehavior {
					return harness.SessionBehavior{Run: func(ctx context.Context, invocation int) (*api.Output, error) {
						if invocation > 1 {
							return &api.Output{}, nil
						}

						return &api.Output{}, run.Wait(ctx)
					}}
				},
				Debugger: harness.DebuggerBehavior{Command: func(ctx context.Context, method string, _ int) (*debugger.Event, error) {
					if method == "Continue" {
						return &debugger.Event{Reason: debugger.ReasonCompleted, Output: &api.Output{}}, command.Wait(ctx)
					}

					return &debugger.Event{Reason: debugger.ReasonEntry}, nil
				}},
			}}))

			plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
			if err != nil {
				t.Fatal(err)
			}

			h.Own(plan)

			session, err := plan.NewSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			h.Own(session)

			debug, err := plan.NewDebugSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			h.Own(debug)

			if _, err := debug.Start(h.Context()); err != nil {
				t.Fatal(err)
			}

			results := make(chan error, 2)
			go func() { _, err := session.Run(h.Context()); results <- err }()
			go func() { _, err := debug.Continue(h.Context()); results <- err }()
			harness.Await(t, run.Started)
			harness.Await(t, command.Started)
			closeParent := plan.Close

			if owner == "runtime" {
				closeParent = h.Runtime().Close
			}

			if err := closeParent(); err != nil {
				t.Fatal(err)
			}

			snapshot := h.RuntimeSpy().Recorder().Snapshot()
			for _, kind := range []string{"session", "debugger"} {
				if snapshot.Count(snapshot.OfKind(kind)[0].ID, "Close") != 0 {
					t.Fatalf("%s Close closed %s", owner, kind)
				}
			}

			if owner == "plan" {
				if child, err := plan.NewSession(h.Context()); err == nil {
					h.Own(child)
					t.Fatal("closed plan accepted a constructor")
				}
			} else {
				if _, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 2"}); err == nil {
					t.Fatal("closed runtime accepted work")
				}

				extra, err := plan.NewSession(h.Context())
				if err != nil {
					t.Fatal(err)
				}

				h.Own(extra)

				if err := extra.Close(); err != nil {
					t.Fatal(err)
				}
			}

			run.Release()
			command.Release()
			for range 2 {
				if err := harness.Await(t, results); err != nil {
					t.Fatalf("parent Close cancelled caller work: %v", err)
				}
			}

			if _, err := session.Run(h.Context()); err != nil {
				t.Fatalf("surviving session cannot run again: %v", err)
			}

			for _, closeResource := range []func() error{debug.Close, session.Close, plan.Close, h.Runtime().Close} {
				if err := closeResource(); err != nil {
					t.Fatal(err)
				}
			}

			h.RuntimeSpy().Recorder().AssertClosed(t)
		})
	}
}

func TestConcurrentSiblingSessionsRemainIndependent(t *testing.T) {
	blocks := []*harness.Block{harness.NewBlock(t), harness.NewBlock(t)}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Session: func(options harness.SessionOptions) harness.SessionBehavior {
		index := int(options.Params["index"].(int64))

		return harness.SessionBehavior{Run: func(ctx context.Context, _ int) (*api.Output, error) {
			return &api.Output{ContentType: "text/plain", Content: []byte(fmt.Sprint(index))}, blocks[index].Wait(ctx)
		}}
	}}}))

	plan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN @index"})
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	var sessions []api.Session

	for index := range 2 {
		session, err := plan.NewSession(h.Context(), api.WithParam("index", int64(index)))
		if err != nil {
			t.Fatal(err)
		}

		h.Own(session)

		sessions = append(sessions, session)
	}

	type result struct {
		output *api.Output
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

	h.Own(plan)

	results := make(chan error, workers)

	for range workers {
		go func() {
			session, err := plan.NewSession(h.Context())
			h.Own(session)

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
			h.Own(plan)

			if err != nil {
				results <- err

				return
			}

			session, err := plan.NewSession(h.Context())
			h.Own(session)

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
