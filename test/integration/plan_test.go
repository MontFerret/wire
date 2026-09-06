package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestCompileRoundTrip(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, test := range []struct {
			name    string
			options []api.PlanOption
			want    harness.CompileOptions
		}{
			{"omitted", nil, harness.CompileOptions{}},
			{"none", []api.PlanOption{api.WithOptimizationLevel(api.OptimizationNone)}, harness.CompileOptions{Level: api.OptimizationNone, HasLevel: true}},
			{"basic", []api.PlanOption{api.WithOptimizationLevel(api.OptimizationBasic)}, harness.CompileOptions{Level: api.OptimizationBasic, HasLevel: true}},
			{"full", []api.PlanOption{api.WithOptimizationLevel(api.OptimizationFull)}, harness.CompileOptions{Level: api.OptimizationFull, HasLevel: true}},
			{"aggressive", []api.PlanOption{api.WithOptimizationLevel(api.OptimizationAggressive)}, harness.CompileOptions{Level: api.OptimizationAggressive, HasLevel: true}},
		} {
			t.Run(map[bool]string{false: "normal/", true: "debug/"}[debug]+test.name, func(t *testing.T) {
				h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Params: []string{"input", "other"}}}))
				compile, method := h.Runtime().Compile, "Compile"

				if debug {
					compile, method = h.Runtime().CompileDebug, "CompileDebug"
				}

				src := api.Source{Name: "folder/query.fql", Content: "RETURN @input + @other\n"}

				plan, err := compile(h.Context(), src, test.options...)
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(plan.Params(), []string{"input", "other"}) {
					t.Fatalf("Params=%v", plan.Params())
				}

				plan.Params()[0] = "changed"

				if plan.Params()[0] != "input" {
					t.Fatal("Params was not defensive")
				}

				snapshot := h.RuntimeSpy().Recorder().Snapshot()
				if snapshot.Count(h.RuntimeSpy().ID(), method) != 1 || len(snapshot.OfKind("plan")) != 1 {
					t.Fatalf("unexpected compilation: %+v", snapshot)
				}

				for _, call := range snapshot.Calls {
					if call.Method == method && (call.Source != src || call.Compile != test.want) {
						t.Fatalf("compile call changed: %+v", call)
					}
				}
			})
		}
	}
}

func TestCompileOptionsApplyOnceBeforeDispatch(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, outcome := range []string{"success", "invalid level", "callback errors", "callback cancellation", "already cancelled"} {
			t.Run(map[bool]string{false: "normal/", true: "debug/"}[debug]+outcome, func(t *testing.T) {
				h := harness.New(t)
				ctx, cancel := context.WithCancel(h.Context())
				defer cancel()

				first, second := errors.New("first"), errors.New("second")
				var order []int
				options := []api.PlanOption{func(o api.PlanOptions) error {
					order = append(order, 1)

					if outcome == "callback cancellation" {
						cancel()
					}

					if outcome == "callback errors" {
						return first
					}

					if outcome == "invalid level" {
						return o.SetOptimizationLevel(99)
					}

					return o.SetOptimizationLevel(api.OptimizationBasic)
				}, nil, func(o api.PlanOptions) error {
					order = append(order, 2)

					if outcome == "callback errors" {
						return second
					}

					return o.SetOptimizationLevel(api.OptimizationNone)
				}}

				if outcome == "already cancelled" {
					cancel()
				}

				compile, operation := h.Runtime().Compile, harness.Compile

				if debug {
					compile, operation = h.Runtime().CompileDebug, harness.CompileDebug
				}

				plan, err := compile(ctx, api.Source{Content: "RETURN 1"}, options...)

				if outcome == "already cancelled" {
					if len(order) != 0 {
						t.Fatalf("cancelled call applied options: %v", order)
					}
				} else if !reflect.DeepEqual(order, []int{1, 2}) {
					t.Fatalf("option order=%v", order)
				}

				if outcome == "success" {
					if err != nil {
						t.Fatal(err)
					}

					if err := plan.Close(); err != nil {
						t.Fatal(err)
					}

					if h.Faults().Count(operation) != 1 {
						t.Fatal("expected one compilation")
					}

					for _, call := range h.RuntimeSpy().Recorder().Snapshot().Calls {
						if call.Method == "Compile" || call.Method == "CompileDebug" {
							if call.Compile != (harness.CompileOptions{HasLevel: true, Level: api.OptimizationNone}) {
								t.Fatalf("final option changed: %+v", call)
							}
						}
					}
				} else {
					if err == nil || plan != nil || h.Faults().Count(operation) != 0 {
						t.Fatalf("invalid options dispatched: %v", err)
					}

					if outcome == "callback errors" && (!errors.Is(err, first) || !errors.Is(err, second)) {
						t.Fatalf("option errors not joined: %v", err)
					}

					if (outcome == "callback cancellation" || outcome == "already cancelled") && !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation lost: %v", err)
					}
				}
			})
		}
	}
}

func TestReusablePlanAndDurableSessions(t *testing.T) {
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Session: func(options harness.SessionOptions) harness.SessionBehavior {
		return harness.SessionBehavior{Run: func(context.Context, int) (api.Output, error) {
			return api.Output{ContentType: options.ContentType, Content: []byte(options.Params["input"].(string))}, nil
		}}
	}}}))

	plan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN @input"})
	if err != nil {
		t.Fatal(err)
	}

	var sessions []api.Session

	for _, value := range []string{"first", "second", "third"} {
		session, err := plan.NewSession(h.Context(), api.WithParam("input", value), api.WithOutputContentType("text/plain"))
		if err != nil {
			t.Fatal(err)
		}

		sessions = append(sessions, session)

		for range 2 {
			output, err := session.Run(h.Context())
			if err != nil || string(output.Content) != value || output.ContentType != "text/plain" {
				t.Fatalf("durable output=%+v err=%v", output, err)
			}
		}
	}

	snapshot := h.RuntimeSpy().Recorder().Snapshot()

	plans, hostedSessions := snapshot.OfKind("plan"), snapshot.OfKind("session")
	if len(plans) != 1 || len(hostedSessions) != 3 || snapshot.Count(h.RuntimeSpy().ID(), "Compile") != 1 {
		t.Fatalf("plan or session recreated: %+v", snapshot)
	}

	for _, resource := range hostedSessions {
		if resource.Parent != plans[0].ID || snapshot.Count(resource.ID, "Run") != 2 || snapshot.Count(resource.ID, "Close") != 0 {
			t.Fatalf("durable session changed: %+v", resource)
		}
	}

	for index, session := range sessions {
		output, err := session.Run(h.Context())
		if err != nil || string(output.Content) != []string{"first", "second", "third"}[index] {
			t.Fatalf("later session creation changed an earlier session's parameters: %+v, %v", output, err)
		}
	}

	if err := sessions[0].Close(); err != nil {
		t.Fatal(err)
	}

	if err := sessions[0].Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := sessions[0].Run(h.Context()); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("Run after Close: %v", err)
	}

	if _, err := sessions[1].Run(h.Context()); err != nil {
		t.Fatalf("sibling session invalidated: %v", err)
	}

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := plan.NewSession(h.Context()); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("NewSession after Close: %v", err)
	}

	h.RuntimeSpy().Recorder().AssertClosed(t)
}
