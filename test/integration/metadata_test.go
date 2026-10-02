package integration_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestRuntimeVersionSnapshot(t *testing.T) {
	for _, value := range []api.Version{"v2.0.0-alpha.test", " runtime-2.0+opaque \n", "版本-α", "\xff", "runtime\x00\xff\xc3", ""} {
		t.Run(string(value), func(t *testing.T) {
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Version: func(context.Context) (api.Version, error) { return value, nil }}),
				harness.WithServerOptions(server.WithRuntimeIdentity(server.RuntimeIdentity{Name: "host", Version: "host-7.1"})))
			for range 3 {
				got, err := h.Runtime().Version(h.Context())
				if err != nil || got != value {
					t.Fatalf("Version=%q, %v; want %q", got, err, value)
				}
			}

			if h.RuntimeSpy().Recorder().Snapshot().Count(h.RuntimeSpy().ID(), "Version") != 1 {
				t.Fatal("cached Version called hosted implementation again")
			}

			if err := h.Runtime().Close(); err != nil {
				t.Fatal(err)
			}

			if err := h.CloseTransport(); err != nil {
				t.Fatal(err)
			}

			got, err := h.Runtime().Version(h.Context())
			if err != nil || got != value {
				t.Fatalf("Version after cleanup=%q, %v", got, err)
			}
		})
	}
}

func TestRuntimeVersionFailureReclaimsConnection(t *testing.T) {
	for _, mode := range []string{"error", "panic", "canceled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			limits := server.DefaultLimits()
			limits.MaxConnections = 1

			h := harness.New(t, harness.WithServerOptions(server.WithLimits(limits)), harness.WithBehavior(harness.RuntimeBehavior{Version: func(context.Context) (api.Version, error) {
				if calls.Add(1) != 2 {
					return "runtime-test", nil
				}

				switch mode {
				case "panic":
					panic("private version")
				case "canceled":
					return "", context.Canceled
				case "deadline":
					return "", context.DeadlineExceeded
				default:
					return "", errors.New("private version")
				}
			}}))
			if err := h.Runtime().Close(); err != nil {
				t.Fatal(err)
			}

			remote, err := h.OpenRuntime()
			if remote != nil || err == nil || strings.Contains(err.Error(), "private") {
				t.Fatalf("failed handshake=%v, %v", remote, err)
			}

			wantCode := codes.Internal
			switch mode {
			case "canceled":
				wantCode = codes.Canceled
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation: %v", err)
				}
			case "deadline":
				wantCode = codes.DeadlineExceeded
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost deadline: %v", err)
				}
			default:
				var wireErr *client.Error
				if !errors.As(err, &wireErr) || wireErr.Category != failure.CategoryInternalRuntime {
					t.Fatalf("lost runtime boundary category: %v", err)
				}
			}

			if status.Code(err) != wantCode {
				t.Fatalf("handshake code=%v; want %v", status.Code(err), wantCode)
			}

			if _, err := h.OpenRuntime(); err != nil {
				t.Fatalf("failed handshake retained connection quota: %v", err)
			}
		})
	}
}

func TestConstructionCancellationReachesRuntimeVersion(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "deadline"}[deadline], func(t *testing.T) {
			var calls atomic.Int32
			block := harness.NewBlock(t)
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Version: func(ctx context.Context) (api.Version, error) {
				if calls.Add(1) == 1 {
					return "runtime-test", nil
				}

				return "", block.Wait(ctx)
			}}))
			ctx, cancel := context.WithCancel(h.Context())
			defer cancel()

			if deadline {
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}

			finished := make(chan error, 1)
			go func() {
				remote, err := client.New(ctx, h.Faults())
				if remote != nil {
					err = errors.Join(errors.New("canceled handshake returned a runtime"), remote.Close())
				}

				finished <- err
			}()
			harness.Await(t, block.Started)

			if !deadline {
				cancel()
			}

			want := context.Canceled

			if deadline {
				want = context.DeadlineExceeded
			}

			if err := harness.Await(t, finished); !errors.Is(err, want) {
				t.Fatalf("construction error=%v; want %v", err, want)
			}

			harness.Await(t, block.Cancelled)
			harness.Await(t, block.Finished)
		})
	}
}

func TestCachedMetadataContextsAndPlanLifecycle(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, params := range [][]string{nil, {"second", "first"}} {
			name := map[bool]string{false: "normal", true: "debug"}[debug]

			if len(params) == 0 {
				name += "/empty"
			}

			t.Run(name, func(t *testing.T) {
				h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Params: params}}))
				compile := h.Runtime().Compile

				if debug {
					compile = h.Runtime().CompileDebug
				}

				plan, err := compile(h.Context(), api.Source{})
				if err != nil {
					t.Fatal(err)
				}

				h.Own(plan)
				read := func() {
					got, err := plan.Params(h.Context())
					if err != nil || !reflect.DeepEqual(got, params) {
						t.Fatalf("Params=%v, %v; want %v", got, err, params)
					}

					if len(got) != 0 {
						got[0] = "caller mutation"
					}
				}
				read()
				read()

				canceled, cancel := context.WithCancel(h.Context())
				cancel()
				expired, expire := context.WithDeadline(h.Context(), time.Now().Add(-time.Second))
				defer expire()
				for _, test := range []struct {
					ctx context.Context
					err error
				}{{nil, nil}, {canceled, context.Canceled}, {expired, context.DeadlineExceeded}} {
					for _, operation := range []func(context.Context) error{
						func(ctx context.Context) error { _, err := h.Runtime().Version(ctx); return err },
						func(ctx context.Context) error { _, err := plan.Params(ctx); return err },
					} {
						if err := operation(test.ctx); err == nil || (test.err != nil && !errors.Is(err, test.err)) {
							t.Fatalf("metadata context error=%v; want %v", err, test.err)
						}
					}
				}

				if err := plan.Close(); err != nil {
					t.Fatal(err)
				}

				if err := h.Runtime().Close(); err != nil {
					t.Fatal(err)
				}

				if err := h.CloseTransport(); err != nil {
					t.Fatal(err)
				}

				before := h.RuntimeSpy().Recorder().Snapshot()
				read()
				read()

				if _, err := h.Runtime().Version(h.Context()); err != nil {
					t.Fatal(err)
				}

				after := h.RuntimeSpy().Recorder().Snapshot()
				if !reflect.DeepEqual(before, after) || after.Count(after.OfKind("plan")[0].ID, "Params") != 1 {
					t.Fatal("cached metadata contacted the hosted implementation")
				}
			})
		}
	}
}
