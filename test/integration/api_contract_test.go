package integration_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestOutputPresenceAndCleanupErrors(t *testing.T) {
	for _, direct := range []bool{true, false} {
		for _, test := range []struct {
			name   string
			output *api.Output
			err    error
		}{
			{"absent failure", nil, errors.New("private execution failure")},
			{"absent success invalid", nil, nil},
			{"present empty", &api.Output{}, nil},
			{"present failure", &api.Output{Content: []byte("partial")}, errors.New("private failure")},
		} {
			t.Run(map[bool]string{true: "runtime/", false: "session/"}[direct]+test.name, func(t *testing.T) {
				h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{
					Run: func(context.Context, api.Source, harness.SessionOptions) (*api.Output, error) {
						return test.output, test.err
					},
					Plan: harness.PlanBehavior{Session: func(harness.SessionOptions) harness.SessionBehavior {
						return harness.SessionBehavior{Run: func(context.Context, int) (*api.Output, error) { return test.output, test.err }}
					}},
				}))
				run := func() (*api.Output, error) { return h.Runtime().Run(h.Context(), api.Source{}) }

				if !direct {
					plan, err := h.Runtime().Compile(h.Context(), api.Source{})
					if err != nil {
						t.Fatal(err)
					}

					h.Own(plan)

					session, err := plan.NewSession(h.Context())
					if err != nil {
						t.Fatal(err)
					}

					h.Own(session)
					run = func() (*api.Output, error) { return session.Run(h.Context()) }
				}

				output, err := run()
				if !reflect.DeepEqual(output, test.output) || (err != nil) != (test.err != nil || test.output == nil) {
					t.Fatalf("output=%#v error=%v", output, err)
				}

				if err != nil && strings.Contains(err.Error(), "private") {
					t.Fatalf("unsanitized error: %v", err)
				}

				releaseErr := status.Error(codes.Unavailable, "release acknowledgement lost")
				h.Faults().FailResponse(harness.ReleaseExecution, releaseErr)

				output, err = run()
				if !reflect.DeepEqual(output, test.output) || err == nil {
					t.Fatalf("cleanup discarded output: %#v %v", output, err)
				}
			})
		}
	}
}

func TestMetadataFailureClosesUnpublishedPlan(t *testing.T) {
	for _, mode := range []string{"error", "panic", "error and close error"} {
		t.Run(mode, func(t *testing.T) {
			behavior := harness.PlanBehavior{Metadata: func() ([]string, error) {
				if mode == "panic" {
					panic("private metadata")
				}

				return []string{"partial"}, errors.New("private metadata")
			}}

			if mode == "error and close error" {
				behavior.Close = func() error { return errors.New("private cleanup") }
			}

			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: behavior}))
			for range 2 {
				plan, err := h.Runtime().Compile(h.Context(), api.Source{})
				if plan != nil || err == nil || strings.Contains(err.Error(), "private") {
					t.Fatalf("unpublished plan=%v err=%v", plan, err)
				}
			}

			h.RuntimeSpy().Recorder().AssertClosed(t)

			if h.Faults().Count(harness.ReleasePlan) != 0 {
				t.Fatal("unpublished plan acquired a transport handle")
			}
		})
	}
}

func TestOptionPresenceAndAnonymousSources(t *testing.T) {
	for _, mode := range []string{"runtime", "session", "debugger"} {
		for _, present := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/omitted", true: "/empty"}[present], func(t *testing.T) {
				h := harness.New(t)
				var options []api.SessionOption

				if present {
					options = []api.SessionOption{api.WithFSRoot("old"), api.WithFSRoot(""), api.WithOutputContentType("old"), api.WithOutputContentType("")}
				}

				method := "Run"

				if mode == "runtime" {
					if _, err := h.Runtime().Run(h.Context(), api.Source{}, options...); err != nil {
						t.Fatal(err)
					}
				} else {
					plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{})
					if err != nil {
						t.Fatal(err)
					}

					h.Own(plan)

					if mode == "session" {
						method = "NewSession"

						session, err := plan.NewSession(h.Context(), options...)
						if err != nil {
							t.Fatal(err)
						}

						h.Own(session)
					} else {
						method = "NewDebugSession"

						session, err := plan.NewDebugSession(h.Context(), options...)
						if err != nil {
							t.Fatal(err)
						}

						h.Own(session)
					}
				}

				var observed bool
				for _, call := range h.RuntimeSpy().Recorder().Snapshot().Calls {
					if call.Source.Name != "" {
						t.Fatalf("anonymous source renamed: %+v", call)
					}

					if call.Method == method {
						observed = true

						if (call.Options.FSRoot != nil) != present || call.Options.ContentTypeSet != present || call.Options.ContentType != "" {
							t.Fatalf("option presence changed: %+v", call.Options)
						}

						if present && *call.Options.FSRoot != "" {
							t.Fatal("root normalized")
						}
					}
				}

				if !observed {
					t.Fatal("hosted operation missing")
				}
			})
		}
	}
}

func TestDebuggerReplacementAndRetainedEnumeration(t *testing.T) {
	running := harness.NewBlock(t)
	limits := server.DefaultLimits()
	limits.MaxBreakpointsPerDebugSession = 4
	h := harness.New(t, harness.WithServerOptions(server.WithLimits(limits)), harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{
		Command: func(ctx context.Context, method string, _ int) (*debugger.Event, error) {
			if method == "Start" {
				return &debugger.Event{Reason: debugger.ReasonEntry}, nil
			}

			if err := running.Wait(ctx); err != nil {
				return nil, err
			}

			return &debugger.Event{Reason: debugger.ReasonBreakpoint, HitBreakpointIDs: []debugger.BreakpointID{1}}, nil
		},
		Frames: []debugger.Frame{{Name: "anonymous", FunctionID: debugger.NoFunction}},
	}}}))

	plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Name: "launch.fql"})
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	session, err := plan.NewDebugSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(session)
	requests := []debugger.BreakpointRequest{{Position: source.Position{Line: 2}}, {Position: source.Position{Line: 2}}, {Position: source.Position{Line: 100}, Options: debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindExact}}}

	initial, err := session.ReplaceBreakpoints(h.Context(), "", requests)
	if err != nil {
		t.Fatal(err)
	}

	if len(initial) != 3 || initial[0].ID == initial[1].ID || initial[2].Bound || initial[2].FunctionID != debugger.NoFunction || initial[0].RequestedLocation.SourceName != "launch.fql" {
		t.Fatalf("replacement metadata=%+v", initial)
	}

	if _, err := session.SetBreakpoint(h.Context(), source.Location{SourceName: "other.fql", Position: source.Position{Line: 1}}); err != nil {
		t.Fatal(err)
	}

	again, err := session.ReplaceBreakpoints(h.Context(), "launch.fql", requests)
	if err != nil || !reflect.DeepEqual(again, initial) {
		t.Fatalf("stable duplicates=%+v %v", again, err)
	}

	if _, err := session.ReplaceBreakpoints(h.Context(), "", append(requests, requests[0])); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("cross-source limit=%v", err)
	}

	if _, err := session.Start(h.Context()); err != nil {
		t.Fatal(err)
	}

	frames, err := session.Frames(h.Context())
	if err != nil || len(frames) != 1 || frames[0].FunctionID != debugger.NoFunction {
		t.Fatalf("signed frames=%+v %v", frames, err)
	}

	result := make(chan *debugger.Event, 1)
	failed := make(chan error, 1)
	go func() { event, err := session.Continue(h.Context()); result <- event; failed <- err }()
	harness.Await(t, running.Started)

	if _, err := session.ReplaceBreakpoints(h.Context(), "", nil); err != nil {
		t.Fatal(err)
	}

	if values := requireBreakpoints(t, session); len(values) != 1 || values[0].RequestedLocation.SourceName != "other.fql" {
		t.Fatalf("empty clearing=%+v", values)
	}

	running.Release()

	if err := harness.Await(t, failed); err != nil {
		t.Fatal(err)
	}

	if event := harness.Await(t, result); !reflect.DeepEqual(event.HitBreakpointIDs, []debugger.BreakpointID{1}) {
		t.Fatalf("old hit IDs lost: %+v", event)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	values := requireBreakpoints(t, session)

	values[0].ID = 999
	if requireBreakpoints(t, session)[0].ID == 999 {
		t.Fatal("retained enumeration aliases returned data")
	}

	ctx, cancel := context.WithCancel(h.Context())
	cancel()

	if _, err := session.Breakpoints(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed enumeration context=%v", err)
	}

	//nolint:staticcheck // Verify the required nil-context rejection.
	if _, err := session.Breakpoints(nil); err == nil {
		t.Fatal("nil enumeration context accepted")
	}
}

func TestDebuggerCommandEventAndError(t *testing.T) {
	for _, commandErr := range []error{errors.New("private cleanup"), context.Canceled, context.DeadlineExceeded, errors.Join(errors.New("private joined cleanup"), context.Canceled)} {
		t.Run(commandErr.Error(), func(t *testing.T) {
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{Command: func(context.Context, string, int) (*debugger.Event, error) {
				return &debugger.Event{Reason: debugger.ReasonCompleted, Output: &api.Output{Content: []byte("done")}}, commandErr
			}}}}))

			plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			h.Own(plan)

			session, err := plan.NewDebugSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			h.Own(session)

			event, err := session.Start(h.Context())
			if event == nil || event.Output == nil || string(event.Output.Content) != "done" || event.Error != nil || err == nil || strings.Contains(err.Error(), "private") {
				t.Fatalf("event=%+v error=%v", event, err)
			}

			for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
				if errors.Is(commandErr, cause) && !errors.Is(err, cause) {
					t.Fatalf("context identity lost: %v", err)
				}
			}
		})
	}
}

func TestStartContextSurvivesInitialStop(t *testing.T) {
	hosted := make(chan context.Context, 1)
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{Command: func(ctx context.Context, _ string, _ int) (*debugger.Event, error) {
		hosted <- ctx

		return &debugger.Event{Reason: debugger.ReasonEntry}, nil
	}}}}))

	plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	session, err := plan.NewDebugSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(session)
	ctx, cancel := context.WithCancel(h.Context())
	defer cancel()

	if _, err := session.Start(ctx); err != nil {
		t.Fatal(err)
	}

	actual := harness.Await(t, hosted)
	if actual.Err() != nil {
		t.Fatal("initial stop ended Start lifetime")
	}

	cancel()
	harness.Await(t, actual.Done())

	if _, err := session.Breakpoints(h.Context()); err != nil {
		t.Fatalf("cancellation released debugger: %v", err)
	}
}

func TestDeferredRuntimeCloseRetainsItsResult(t *testing.T) {
	h := harness.New(t)

	plan, err := h.Runtime().Compile(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	if h.Faults().Count(harness.CloseRuntime) != 0 {
		t.Fatal("runtime released a retained plan")
	}

	if _, err := h.Runtime().Compile(h.Context(), api.Source{}); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("runtime admission=%v", err)
	}

	cleanupErr := status.Error(codes.Unavailable, "late teardown failed")
	h.Faults().FailResponse(harness.CloseRuntime, cleanupErr)
	h.ExpectCleanupError(cleanupErr)

	if err := plan.Close(); !errors.Is(err, cleanupErr) {
		t.Fatalf("final child lost cleanup error: %v", err)
	}

	if err := h.Runtime().Close(); err != nil {
		t.Fatalf("earlier close result changed: %v", err)
	}
}

func TestDebuggerContextsAndDirectOperations(t *testing.T) {
	observed := make(chan string, 16)
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{Observe: func(ctx context.Context, method string) error {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("missing request deadline")
		}

		observed <- method

		return nil
	}}}}))

	plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	session, err := plan.NewDebugSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(session)

	if _, err := session.Start(h.Context()); err != nil {
		t.Fatal(err)
	}

	location := source.Location{Position: source.Position{Line: 1}}
	operations := []func(context.Context) error{
		func(ctx context.Context) error { _, err := session.Start(ctx); return err }, func(ctx context.Context) error { _, err := session.Continue(ctx); return err },
		func(ctx context.Context) error { _, err := session.StepIn(ctx); return err }, func(ctx context.Context) error { _, err := session.StepOver(ctx); return err }, func(ctx context.Context) error { _, err := session.StepOut(ctx); return err },
		session.Pause, func(ctx context.Context) error { _, err := session.SetBreakpoint(ctx, location); return err }, func(ctx context.Context) error {
			_, err := session.SetBreakpointAt(ctx, location, debugger.BreakpointOptions{})

			return err
		},
		func(ctx context.Context) error { _, err := session.ReplaceBreakpoints(ctx, "", nil); return err }, func(ctx context.Context) error { return session.DeleteBreakpoint(ctx, 1) },
		func(ctx context.Context) error { _, err := session.Breakpoints(ctx); return err }, func(ctx context.Context) error { _, err := session.Frames(ctx); return err },
		func(ctx context.Context) error { _, err := session.Locals(ctx); return err }, func(ctx context.Context) error { _, err := session.FrameLocals(ctx, 0); return err },
		func(ctx context.Context) error { _, err := session.Variables(ctx, 9); return err }, func(ctx context.Context) error { _, err := session.Evaluate(ctx, "x"); return err }, func(ctx context.Context) error { _, err := session.EvaluateFrame(ctx, 0, "x"); return err },
	}
	ctx, cancel := context.WithCancel(h.Context())
	cancel()
	before := len(h.RuntimeSpy().Recorder().Snapshot().Calls)
	for i, operation := range operations {
		if err := operation(nil); err == nil {
			t.Fatalf("method %d accepted nil context", i)
		}

		if err := operation(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("method %d cancellation=%v", i, err)
		}
	}

	if len(h.RuntimeSpy().Recorder().Snapshot().Calls) != before {
		t.Fatal("invalid context reached hosted debugger")
	}

	if _, err := session.Locals(h.Context()); err != nil {
		t.Fatal(err)
	}

	if _, err := session.Evaluate(h.Context(), "x"); err != nil {
		t.Fatal(err)
	}

	if _, err := session.SetBreakpoint(h.Context(), location); err != nil {
		t.Fatal(err)
	}

	snapshot := h.RuntimeSpy().Recorder().Snapshot()
	id := snapshot.OfKind("debugger")[0].ID
	for _, method := range []string{"Locals", "Evaluate", "SetBreakpoint"} {
		if snapshot.Count(id, method) != 1 {
			t.Fatalf("%s not projected directly", method)
		}
	}

	for range 3 {
		harness.Await(t, observed)
	}
}

func TestDebuggerEnumerationFailureAfterClose(t *testing.T) {
	var closed atomic.Bool
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{
		Enumeration: func(context.Context) error {
			if closed.Load() {
				return errors.New("private final enumeration")
			}

			return nil
		},
		Close: func() error { closed.Store(true); return nil },
	}}}))

	plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	session, err := plan.NewDebugSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(session)

	if _, err := session.Breakpoints(h.Context()); err != nil {
		t.Fatal(err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	before := len(h.RuntimeSpy().Recorder().Snapshot().Calls)
	for range 2 {
		values, err := session.Breakpoints(h.Context())
		if values != nil || err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("retained enumeration=%v %v", values, err)
		}
	}

	if before != len(h.RuntimeSpy().Recorder().Snapshot().Calls) {
		t.Fatal("closed enumeration contacted hosted debugger")
	}
}

func TestRuntimeClosePreservesAdmittedCompile(t *testing.T) {
	block := harness.NewBlock(t)
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Compile: func(ctx context.Context, _ api.Source, _ bool, _ harness.CompileOptions) error {
		return block.Wait(ctx)
	}}))
	plans := make(chan api.Plan, 1)
	errs := make(chan error, 1)
	go func() { plan, err := h.Runtime().Compile(h.Context(), api.Source{}); plans <- plan; errs <- err }()
	harness.Await(t, block.Started)

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	if h.Faults().Count(harness.CloseRuntime) != 0 {
		t.Fatal("admitted compile lost connection")
	}

	block.Release()

	if err := harness.Await(t, errs); err != nil {
		t.Fatal(err)
	}

	plan := harness.Await(t, plans)
	h.Own(plan)

	session, err := plan.NewSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(session)

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := session.Run(h.Context()); err != nil {
		t.Fatal(err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	if h.Faults().Count(harness.CloseRuntime) != 1 {
		t.Fatal("last child did not release connection")
	}
}
