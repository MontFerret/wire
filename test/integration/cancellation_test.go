package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCancellationReachesHostedOperations(t *testing.T) {
	for _, operation := range []string{"runtime", "session", "Start", "Continue", "Evaluate"} {
		t.Run(operation, func(t *testing.T) {
			block := harness.NewBlock(t)
			behavior := harness.RuntimeBehavior{
				Run: func(ctx context.Context, src api.Source, _ harness.SessionOptions) (api.Output, error) {
					if src.Content == "RETURN 2" {
						return api.Output{}, nil
					}

					return api.Output{}, block.Wait(ctx)
				},
				Plan: harness.PlanBehavior{
					Session: func(harness.SessionOptions) harness.SessionBehavior {
						return harness.SessionBehavior{Run: func(ctx context.Context, _ int) (api.Output, error) { return api.Output{}, block.Wait(ctx) }}
					},
					Debugger: harness.DebuggerBehavior{
						Command: func(ctx context.Context, method string, _ int) (*debugger.Event, error) {
							if method == operation {
								return nil, block.Wait(ctx)
							}

							return &debugger.Event{Reason: debugger.ReasonEntry}, nil
						},
						Evaluate: func(ctx context.Context, _ int, _ string) (debugger.Value, error) {
							return debugger.Value{}, block.Wait(ctx)
						},
					},
				},
			}
			limits := server.DefaultLimits()
			limits.MaxExecutionsPerConnection = 1
			h := harness.New(t, harness.WithBehavior(behavior), harness.WithServerOptions(server.WithLimits(limits)))
			ctx, cancel := context.WithCancel(h.Context())
			defer cancel()
			run := func() error {
				_, err := h.Runtime().Run(ctx, api.Source{Content: "RETURN 1"})

				return err
			}

			if operation != "runtime" {
				plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
				if err != nil {
					t.Fatal(err)
				}

				if operation == "session" {
					session, err := plan.NewSession(h.Context())
					if err != nil {
						t.Fatal(err)
					}

					run = func() error {
						_, err := session.Run(ctx)

						return err
					}
				} else {
					session, err := plan.NewDebugSession(h.Context())
					if err != nil {
						t.Fatal(err)
					}

					if operation != "Start" {
						if _, err := session.Start(h.Context()); err != nil {
							t.Fatal(err)
						}
					}

					switch operation {
					case "Start":
						run = func() error {
							_, err := session.Start(ctx)

							return err
						}
					case "Continue":
						run = func() error {
							_, err := session.Continue(ctx)

							return err
						}
					case "Evaluate":
						run = func() error {
							_, err := session.Evaluate(ctx, "input")

							return err
						}
					}
				}
			}

			result := make(chan error, 1)
			go func() { result <- run() }()
			harness.Await(t, block.Started)
			cancel()
			err := harness.Await(t, result)
			if !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
				t.Fatalf("caller cancellation=%v", err)
			}

			harness.Await(t, block.Cancelled)
			harness.Await(t, block.Finished)

			if operation == "Start" || operation == "Continue" {
				snapshot := h.RuntimeSpy().Recorder().Snapshot()
				if snapshot.Count(snapshot.OfKind("debugger")[0].ID, "Close") != 1 {
					t.Fatal("cancelled debugger not closed before return")
				}
			}

			if operation == "runtime" || operation == "session" {
				if h.Faults().Count(harness.ReleaseExecution) != 1 {
					t.Fatal("cancelled execution was not released")
				}

				if _, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 2"}); err != nil {
					t.Fatalf("cancelled execution retained the only execution slot: %v", err)
				}
			}
		})
	}
}

func TestCompileCancellationPreservesDetachedAllocation(t *testing.T) {
	for _, debug := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "debug"}[debug], func(t *testing.T) {
			block := harness.NewBlock(t)
			hostedContext := make(chan context.Context, 1)
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Compile: func(ctx context.Context, _ api.Source, _ bool, _ harness.CompileOptions) error {
				hostedContext <- ctx

				return block.Wait(ctx)
			}}))
			compile := h.Runtime().Compile

			if debug {
				compile = h.Runtime().CompileDebug
			}

			ctx, cancel := context.WithCancel(h.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				plan, err := compile(ctx, api.Source{Content: "RETURN 1"})
				if plan != nil {
					t.Error("cancelled compile returned a plan")
					err = errors.Join(err, errors.New("cancelled compile returned a plan"), plan.Close())
				}

				result <- err
			}()
			harness.Await(t, block.Started)
			hostCtx := harness.Await(t, hostedContext)
			cancel()

			if hostCtx.Err() != nil {
				t.Fatalf("allocation incorrectly inherited caller cancellation: %v", hostCtx.Err())
			}

			block.Release()

			if err := harness.Await(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("compile cancellation=%v", err)
			}

			snapshot := h.RuntimeSpy().Recorder().Snapshot()
			plans := snapshot.OfKind("plan")
			if len(plans) != 1 || snapshot.Count(plans[0].ID, "Close") != 1 || h.Faults().Count(harness.CloseRuntime) != 0 {
				t.Fatalf("cancelled allocation was not reclaimed narrowly: %+v", snapshot)
			}
		})
	}
}

func TestLogicalShutdownCancelsHostedCompile(t *testing.T) {
	block := harness.NewBlock(t)
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Compile: func(ctx context.Context, _ api.Source, _ bool, _ harness.CompileOptions) error {
		return block.Wait(ctx)
	}}))
	result := make(chan error, 1)
	go func() {
		_, err := h.Runtime().Compile(h.Context(), api.Source{Content: "RETURN 1"})
		result <- err
	}()
	harness.Await(t, block.Started)

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	if err := harness.Await(t, result); err == nil {
		t.Fatal("compile succeeded after logical shutdown")
	}

	harness.Await(t, block.Cancelled)
	harness.Await(t, block.Finished)
	h.RuntimeSpy().Recorder().AssertClosed(t)
}
