package integration_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/test/integration/harness"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnavailableServer(t *testing.T) {
	h := harness.New(t, harness.WithUnavailableServer())
	runtime, err := h.OpenRuntime()
	if runtime != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("unavailable handshake: runtime=%T err=%v", runtime, err)
	}

	var remote *client.Error
	if !errors.As(err, &remote) || remote.Category != 0 {
		t.Fatalf("transport error acquired a hosted category: %v", err)
	}
}

func TestConnectionLossReclaimsResources(t *testing.T) {
	for _, mode := range []string{"runtime", "session", "debugger"} {
		for _, shutdown := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/transport", true: "/server"}[shutdown], func(t *testing.T) {
				block := harness.NewBlock(t)
				h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{
					Run: func(ctx context.Context, _ api.Source, _ harness.SessionOptions) (api.Output, error) {
						return api.Output{}, block.Wait(ctx)
					},
					Plan: harness.PlanBehavior{
						Session: func(harness.SessionOptions) harness.SessionBehavior {
							return harness.SessionBehavior{Run: func(ctx context.Context, _ int) (api.Output, error) { return api.Output{}, block.Wait(ctx) }}
						},
						Debugger: harness.DebuggerBehavior{Command: func(ctx context.Context, _ string, _ int) (*debugger.Event, error) { return nil, block.Wait(ctx) }},
					},
				}))
				run := func() error {
					_, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 1"})

					return err
				}
				var terminalEvent *debugger.Event

				if mode != "runtime" {
					plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
					if err != nil {
						t.Fatal(err)
					}

					if mode == "session" {
						session, err := plan.NewSession(h.Context())
						if err != nil {
							t.Fatal(err)
						}

						run = func() error {
							_, err := session.Run(h.Context())

							return err
						}
					} else {
						session, err := plan.NewDebugSession(h.Context())
						if err != nil {
							t.Fatal(err)
						}

						run = func() error {
							var err error
							terminalEvent, err = session.Start(h.Context())

							return err
						}
					}
				}

				result := make(chan error, 1)
				go func() { result <- run() }()
				harness.Await(t, block.Started)
				stop := h.CloseTransport

				if shutdown {
					stop = h.Shutdown
				}

				if err := stop(); err != nil {
					t.Fatal(err)
				}

				err := harness.Await(t, result)
				if err == nil && (!shutdown || mode != "debugger" || terminalEvent == nil || terminalEvent.Reason != debugger.ReasonTerminated) {
					t.Fatalf("active operation silently succeeded after connection loss: event=%#v", terminalEvent)
				}

				if !expectedConnectionLoss(err, mode, shutdown) {
					t.Fatalf("connection failure classification=%v", err)
				}

				harness.Await(t, block.Cancelled)
				harness.Await(t, block.Finished)
				h.RuntimeSpy().Recorder().AssertClosed(t)
			})
		}
	}
}

func expectedConnectionLoss(err error, mode string, shutdown bool) bool {
	if err == nil {
		return true
	}

	// A wrapped join still contains independent operation and cleanup failures.
	// Inspect every cause before matching a status or sentinel from the tree.
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		for _, cause := range joined.Unwrap() {
			if !expectedConnectionLoss(cause, mode, shutdown) {
				return false
			}
		}

		return true
	}

	if errors.Is(err, client.ErrClosed) || errors.Is(err, client.ErrExecutionCancelled) {
		return true
	}

	switch status.Code(err) {
	case codes.Unavailable, codes.Canceled:
		return true
	case codes.NotFound:
		// Shutdown can remove the connection or execution before Run opens its
		// watch. Debugger commands establish their watch before starting work.
		if !shutdown || (mode != "runtime" && mode != "session") {
			return false
		}

		var remote *client.Error
		if errors.As(err, &remote) {
			return remote.Category == failure.CategoryConnectionNotFound || remote.Category == failure.CategoryExecutionNotFound
		}
	}

	return false
}

func TestWatchTerminationReturnsError(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, watchErr := range []error{io.EOF, status.Error(codes.Unavailable, "watch transport lost")} {
			t.Run(map[bool]string{false: "execution/", true: "debugger/"}[debug]+watchErr.Error(), func(t *testing.T) {
				block := harness.NewBlock(t)
				h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{
					Run: func(ctx context.Context, _ api.Source, _ harness.SessionOptions) (api.Output, error) {
						return api.Output{}, block.Wait(ctx)
					},
					Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{Command: func(ctx context.Context, _ string, _ int) (*debugger.Event, error) { return nil, block.Wait(ctx) }}},
				}))
				operation := harness.WatchExecution
				run := func() error {
					_, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 1"})

					return err
				}
				var closeDebug func() error

				if debug {
					plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
					if err != nil {
						t.Fatal(err)
					}

					session, err := plan.NewDebugSession(h.Context())
					if err != nil {
						t.Fatal(err)
					}

					operation = harness.WatchDebugger
					run = func() error {
						_, err := session.Start(h.Context())

						return err
					}
					closeDebug = session.Close
				}

				h.Faults().EndWatch(operation, watchErr, block.Started)
				err := run()
				if err == nil || (watchErr == io.EOF && !errors.Is(err, io.EOF)) || (watchErr != io.EOF && status.Code(err) != codes.Unavailable) {
					t.Fatalf("watch failure=%v", err)
				}

				if closeDebug != nil {
					if err := closeDebug(); err != nil {
						t.Fatal(err)
					}
				}

				harness.Await(t, block.Started)
				harness.Await(t, block.Cancelled)
				harness.Await(t, block.Finished)
			})
		}
	}
}

func TestDebuggerCommandFailure(t *testing.T) {
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{Command: func(context.Context, string, int) (*debugger.Event, error) {
		return nil, errors.New("private command failure")
	}}}}))
	plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}

	session, err := plan.NewDebugSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	_, err = session.Start(h.Context())
	var remote *failure.Failure
	if !errors.As(err, &remote) || remote.Category != failure.CategoryInternalRuntime || remote.Message != "runtime operation failed" {
		t.Fatalf("debug command failure=%v", err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}
