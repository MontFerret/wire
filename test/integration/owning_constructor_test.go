package integration_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestOwnedTargetsAndStartupContext(t *testing.T) {
	h := harness.New(t, harness.WithOwnedTransport(client.WithInsecure()), harness.WithBehavior(harness.RuntimeBehavior{
		Run: func(context.Context, api.Source, harness.SessionOptions) (*api.Output, error) {
			return &api.Output{ContentType: "application/json", Content: []byte("[42]")}, nil
		},
	}))
	for _, prefix := range []string{"", "passthrough:///", "dns:///"} {
		ctx, cancel := context.WithCancel(h.Context())
		remote, err := client.New(ctx, prefix+h.Endpoint(), client.WithInsecure())
		cancel()

		if err != nil {
			t.Fatal(err)
		}

		h.Own(remote)

		output, err := remote.Run(h.Context(), api.NewSource("query.fql", "RETURN 42"))
		if err != nil || output == nil || output.ContentType != "application/json" || string(output.Content) != "[42]" {
			t.Fatalf("startup cancellation or target changed runtime behavior: %v, %v", output, err)
		}

		if err := remote.Close(); err != nil {
			t.Fatal(err)
		}

		harness.Await(t, h.TransportClosed())
	}

	// The trailing space is part of the gRPC endpoint. Trimming it would turn
	// this failed connection into a successful handshake with the open listener.
	remote, err := client.New(h.Context(), "passthrough:///"+h.Endpoint()+" ", client.WithInsecure())
	if remote != nil {
		h.Own(remote)
	}

	if remote != nil || err == nil {
		t.Fatalf("constructor normalized the supplied target: %v, %v", remote, err)
	}
}

func TestOwnedAdmittedRootCallsSurviveClose(t *testing.T) {
	for _, operation := range []string{"run", "compile"} {
		t.Run(operation, func(t *testing.T) {
			block := harness.NewBlock(t)
			behavior := harness.RuntimeBehavior{}

			if operation == "run" {
				behavior.Run = func(ctx context.Context, _ api.Source, _ harness.SessionOptions) (*api.Output, error) {
					return &api.Output{Content: []byte("retained")}, block.Wait(ctx)
				}
			} else {
				behavior.Compile = func(ctx context.Context, _ api.Source, _ bool, _ harness.CompileOptions) error {
					return block.Wait(ctx)
				}
			}

			h := harness.New(t, harness.WithOwnedTransport(client.WithInsecure()), harness.WithBehavior(behavior))
			type result struct {
				plan   api.Plan
				output *api.Output
				err    error
			}
			done := make(chan result, 1)
			go func() {
				var value result

				if operation == "run" {
					value.output, value.err = h.Runtime().Run(h.Context(), api.NewAnonymousSource("RETURN 1"))
				} else {
					value.plan, value.err = h.Runtime().Compile(h.Context(), api.NewAnonymousSource("RETURN 1"))
				}

				done <- value
			}()
			harness.Await(t, block.Started)

			if err := h.Runtime().Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := h.Runtime().Compile(h.Context(), api.NewAnonymousSource("RETURN 2")); !errors.Is(err, client.ErrClosed) {
				t.Fatalf("closed runtime admitted root work: %v", err)
			}

			assertOwnedChannelRetained(t, h)
			block.Release()

			value := harness.Await(t, done)
			if value.err != nil {
				t.Fatalf("admitted operation failed after runtime Close: %v", value.err)
			}

			if operation == "run" {
				if value.output == nil || string(value.output.Content) != "retained" {
					t.Fatal("admitted run lost its output")
				}
			} else {
				h.Own(value.plan)
				assertOwnedChannelRetained(t, h)

				session, err := value.plan.NewSession(h.Context())
				if err != nil {
					t.Fatal(err)
				}

				h.Own(session)

				if err := value.plan.Close(); err != nil {
					t.Fatal(err)
				}

				assertOwnedChannelRetained(t, h)

				if _, err := session.Run(h.Context()); err != nil {
					t.Fatalf("session lost transport after plan Close: %v", err)
				}

				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
			}

			harness.Await(t, h.TransportClosed())
			h.RuntimeSpy().Recorder().AssertClosed(t)
		})
	}
}

func TestOwnedDeferredReleaseErrorBelongsToFinalResource(t *testing.T) {
	releaseErr := status.Error(codes.PermissionDenied, "release rejected")
	rejectRelease := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/ferret.wire.v1.RuntimeService/CloseConnection" {
			return nil, releaseErr
		}

		return next(ctx, request)
	}
	h := harness.New(t, harness.WithOwnedTransport(client.WithInsecure()), harness.WithServerOptions(server.WithUnaryInterceptors(rejectRelease)))
	h.ExpectCleanupError(releaseErr)

	plan, err := h.Runtime().Compile(h.Context(), api.NewAnonymousSource("RETURN 1"))
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	assertOwnedChannelRetained(t, h)

	if err := plan.Close(); !errors.Is(err, releaseErr) {
		t.Fatalf("final resource lost deferred release error: %v", err)
	}

	harness.Await(t, h.TransportClosed())

	if err := h.Runtime().Close(); err != nil {
		t.Fatalf("deferred release retroactively changed runtime Close: %v", err)
	}
}

func assertOwnedChannelRetained(t *testing.T, h *harness.Harness) {
	t.Helper()
	select {
	case <-h.TransportClosed():
		t.Fatal("channel closed while admitted work or descendants retained it")
	default:
	}
}

func TestOwnedKnownCleanupFailurePreservesRuntime(t *testing.T) {
	releaseErr := status.Error(codes.Unavailable, "execution release unavailable")
	rejectRelease := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/ferret.wire.v1.ExecutionService/ReleaseExecution" {
			return nil, releaseErr
		}

		return next(ctx, request)
	}
	h := harness.New(t, harness.WithOwnedTransport(client.WithInsecure()), harness.WithServerOptions(server.WithUnaryInterceptors(rejectRelease)))
	for range 2 {
		output, err := h.Runtime().Run(h.Context(), api.NewAnonymousSource("RETURN 1"))
		if output == nil || !errors.Is(err, releaseErr) {
			t.Fatalf("known cleanup failure changed output or error: %v, %v", output, err)
		}

		assertOwnedChannelRetained(t, h)
	}

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	harness.Await(t, h.TransportClosed())
}
