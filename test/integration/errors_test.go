package integration_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDiagnosticsAndFailureClassification(t *testing.T) {
	for _, mode := range []string{"compile", "compile debug", "runtime", "session", "debugger"} {
		for count := range 3 {
			t.Run(mode+"/"+[]string{"plain", "single", "multiple"}[count], func(t *testing.T) {
				var values diagnostics.Diagnostics

				for index := range count {
					values = append(values, diagnostics.Diagnostic{Kind: diagnostics.Kind("CustomKind"), Message: "expected expression", Source: source.New("query.fql", "RETURN @input"), Annotations: []diagnostics.Annotation{
						{Range: source.Range{Location: source.Location{SourceName: "query.fql", Position: source.Position{Line: index + 1, Column: 2}}, Span: source.Span{Start: 1, End: 4}}, Message: "primary span", Primary: true},
						{Range: source.Range{Location: source.Location{SourceName: "query.fql", Position: source.Position{Line: 1, Column: 5}}, Span: source.Span{Start: 4, End: 4}}, Message: "secondary span"},
					}, Hint: "provide a value", Note: "portable note"})
				}

				hostErr := errors.Join(errors.New("private-host-secret"), values.Err())
				behavior := harness.RuntimeBehavior{
					Run: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) {
						return api.Output{}, hostErr
					},
					Plan: harness.PlanBehavior{
						Session: func(harness.SessionOptions) harness.SessionBehavior {
							return harness.SessionBehavior{Run: func(context.Context, int) (api.Output, error) { return api.Output{}, hostErr }}
						},
						Debugger: harness.DebuggerBehavior{Command: func(context.Context, string, int) (*debugger.Event, error) {
							return &debugger.Event{Reason: debugger.ReasonRuntimeError, Error: hostErr}, nil
						}},
					},
				}

				if strings.HasPrefix(mode, "compile") {
					behavior.Compile = func(context.Context, api.Source, bool, harness.CompileOptions) error { return hostErr }
				}

				h := harness.New(t, harness.WithBehavior(behavior))
				var err error
				src := api.Source{Name: "query.fql", Content: "RETURN @input"}

				switch mode {
				case "compile":
					_, err = h.Runtime().Compile(h.Context(), src)
				case "compile debug":
					_, err = h.Runtime().CompileDebug(h.Context(), src)
				case "runtime":
					_, err = h.Runtime().Run(h.Context(), src)
				default:
					plan, createErr := h.Runtime().CompileDebug(h.Context(), src)
					if createErr != nil {
						t.Fatal(createErr)
					}

					if mode == "session" {
						session, createErr := plan.NewSession(h.Context())
						if createErr != nil {
							t.Fatal(createErr)
						}

						_, err = session.Run(h.Context())
					} else {
						session, createErr := plan.NewDebugSession(h.Context())
						if createErr != nil {
							t.Fatal(createErr)
						}

						event, commandErr := session.Start(h.Context())
						if commandErr != nil {
							t.Fatal(commandErr)
						}

						err = event.Error
					}
				}

				if err == nil || strings.Contains(err.Error(), "private-host-secret") {
					t.Fatalf("failure absent or unsanitized: %v", err)
				}

				var actual diagnostics.Diagnostics

				if strings.HasPrefix(mode, "compile") {
					var remote *client.Error
					if !errors.As(err, &remote) || remote.Category != failure.CategoryCompilation || status.Code(err) != codes.InvalidArgument || remote.Message != "compilation failed" {
						t.Fatalf("compile classification=%v", err)
					}

					actual = remote.Diagnostics
				} else {
					var remote *failure.Failure
					if !errors.As(err, &remote) || remote.Category != failure.CategoryExecution || remote.Message != "runtime operation failed" {
						t.Fatalf("execution classification=%v", err)
					}

					actual = remote.Diagnostics
				}

				if !reflect.DeepEqual(actual, values) {
					t.Fatalf("diagnostics=%#v want=%#v", actual, values)
				}
			})
		}
	}
}

func TestErrorFamilies(t *testing.T) {
	t.Run("invalid source and portable value", func(t *testing.T) {
		h := harness.New(t)

		if _, err := h.Runtime().Compile(h.Context(), api.Source{}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid source=%v", err)
		}

		if _, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 1"}, api.WithParam("bad", make(chan int))); err == nil {
			t.Fatal("unsupported value accepted")
		}

		if h.Faults().Count(harness.RunRuntime) != 0 {
			t.Fatal("local validation dispatched runtime execution")
		}

		if _, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 2"}); err != nil {
			t.Fatalf("rejection invalidated Runtime: %v", err)
		}
	})
	t.Run("not found and invalid debug state", func(t *testing.T) {
		h := harness.New(t)
		plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
		if err != nil {
			t.Fatal(err)
		}

		session, err := plan.NewDebugSession(h.Context())
		if err != nil {
			t.Fatal(err)
		}

		err = session.DeleteBreakpoint(987)
		var remote *client.Error
		if !errors.As(err, &remote) || remote.Category != failure.CategoryBreakpointNotFound || status.Code(err) != codes.NotFound {
			t.Fatalf("not-found classification=%v", err)
		}

		if _, err := session.Frames(); !errors.As(err, &remote) || remote.Category != failure.CategoryInvalidState || status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("invalid-state classification=%v", err)
		}
	})
	t.Run("deadline exceeded", func(t *testing.T) {
		h := harness.New(t)
		ctx, cancel := context.WithDeadline(h.Context(), time.Now().Add(-time.Second))
		defer cancel()

		if _, err := h.Runtime().Run(ctx, api.Source{Content: "RETURN 1"}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("caller deadline=%v", err)
		}

		if h.Faults().Count(harness.RunRuntime) != 0 {
			t.Fatal("expired call dispatched")
		}
	})
	t.Run("remote cancellation differs from caller cancellation", func(t *testing.T) {
		h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Run: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) {
			return api.Output{}, context.Canceled
		}}))

		if _, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 1"}); !errors.Is(err, client.ErrExecutionCancelled) || errors.Is(err, context.Canceled) {
			t.Fatalf("remote cancellation=%v", err)
		}
	})
	t.Run("protocol resource limit", func(t *testing.T) {
		limits := server.DefaultLimits()
		limits.MaxPlansPerConnection = 1
		h := harness.New(t, harness.WithServerOptions(server.WithLimits(limits)))

		if _, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 1"}); err != nil {
			t.Fatal(err)
		}

		if _, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 2"}); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("limit rejection=%v", err)
		}

		if h.RuntimeSpy().Recorder().Snapshot().Count(h.RuntimeSpy().ID(), "Compile") != 1 {
			t.Fatal("rejected allocation reached host")
		}
	})
}

func TestConstructorPanicPreservesParent(t *testing.T) {
	var attempts atomic.Int32
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{NewSession: func(context.Context, harness.SessionOptions) error {
		if attempts.Add(1) == 1 {
			panic("constructor-secret")
		}

		return nil
	}}}))
	plan, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := plan.NewSession(h.Context()); status.Code(err) != codes.Internal || strings.Contains(err.Error(), "constructor-secret") {
		t.Fatalf("constructor panic=%v", err)
	}

	session, err := plan.NewSession(h.Context())
	if err != nil {
		t.Fatalf("constructor panic invalidated parent: %v", err)
	}

	if _, err := session.Run(h.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPanicContainmentAndResourcePoisoning(t *testing.T) {
	for _, mode := range []string{"runtime", "compile", "session", "debugger", "session close"} {
		t.Run(mode, func(t *testing.T) {
			behavior := harness.RuntimeBehavior{}

			switch mode {
			case "runtime":
				behavior.Run = func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { panic("panic-secret") }
			case "compile":
				behavior.Compile = func(context.Context, api.Source, bool, harness.CompileOptions) error { panic("panic-secret") }
			case "session":
				behavior.Plan.Session = func(harness.SessionOptions) harness.SessionBehavior {
					return harness.SessionBehavior{Run: func(context.Context, int) (api.Output, error) { panic("panic-secret") }}
				}
			case "debugger":
				behavior.Plan.Debugger.Inspect = func(string) error { panic("panic-secret") }
			case "session close":
				behavior.Plan.Session = func(harness.SessionOptions) harness.SessionBehavior {
					return harness.SessionBehavior{Close: func() error { panic("panic-secret") }}
				}
			}

			h := harness.New(t, harness.WithBehavior(behavior))
			var err error

			switch mode {
			case "runtime":
				_, err = h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 1"})
			case "compile":
				_, err = h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 1"})
			default:
				plan, createErr := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
				if createErr != nil {
					t.Fatal(createErr)
				}

				if mode == "debugger" {
					session, createErr := plan.NewDebugSession(h.Context())
					if createErr != nil {
						t.Fatal(createErr)
					}

					if _, startErr := session.Start(h.Context()); startErr != nil {
						t.Fatal(startErr)
					}

					_, err = session.Frames()

					if _, nextErr := session.Frames(); nextErr == nil {
						t.Fatal("poisoned debugger was reused")
					}

					h.RuntimeSpy().Recorder().Wait(t, "panicked debugger closes", func(s harness.Snapshot) bool { return s.Count(s.OfKind("debugger")[0].ID, "Close") == 1 })
					snapshot := h.RuntimeSpy().Recorder().Snapshot()
					if snapshot.Count(snapshot.OfKind("debugger")[0].ID, "Frames") != 1 {
						t.Fatal("a poisoned hosted debugger received another inspection call")
					}
				} else {
					session, createErr := plan.NewSession(h.Context())
					if createErr != nil {
						t.Fatal(createErr)
					}

					if mode == "session close" {
						err = session.Close()

						if second := session.Close(); second == nil {
							t.Fatal("cleanup panic was not retained")
						}
					} else {
						_, err = session.Run(h.Context())

						if _, nextErr := session.Run(h.Context()); status.Code(nextErr) != codes.FailedPrecondition {
							t.Fatalf("poisoned Session reuse=%v", nextErr)
						}
					}
				}
			}

			if err == nil || strings.Contains(err.Error(), "panic-secret") {
				t.Fatalf("panic unsanitized or lost: %v", err)
			}

			var rpc *client.Error
			var terminal *failure.Failure

			if errors.As(err, &rpc) {
				if rpc.Category != failure.CategoryInternalRuntime || status.Code(err) != codes.Internal {
					t.Fatalf("panic RPC=%+v", rpc)
				}
			} else if !errors.As(err, &terminal) || terminal.Category != failure.CategoryInternalRuntime {
				t.Fatalf("panic classification=%v", err)
			}
		})
	}
}
