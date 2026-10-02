package grpcserver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

type (
	versionRuntime struct {
		api.Runtime
		version func(context.Context) (api.Version, error)
	}

	connectStream struct {
		grpc.ServerStream
		ctx      context.Context
		response *wirev1.ConnectResponse
		send     func() error
	}
)

func (r *versionRuntime) Version(ctx context.Context) (api.Version, error) {
	return r.version(ctx)
}

func (s *connectStream) Context() context.Context {
	return s.ctx
}

func (s *connectStream) Send(response *wirev1.ConnectResponse) error {
	s.response = response

	return s.send()
}

func TestConnectProjectsOpaqueRuntimeVersionIndependentlyOfHostIdentity(t *testing.T) {
	for _, version := range []api.Version{"v2.0.0-alpha.test", " runtime-2.0+opaque \n", "版本-α", "\xff", "runtime\x00\xff\xc3", ""} {
		t.Run(string(version), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			calls := 0
			connections := core.NewConnectionRegistry(1, core.ResourceLimits{})
			service := &RuntimeService{
				connections: connections,
				info:        Handshake{ProtocolName: "ferret.wire", ProtocolVersion: "v1", RuntimeName: "host", RuntimeVersion: "host-7.1"},
				runtime: &versionRuntime{version: func(received context.Context) (api.Version, error) {
					calls++
					deadline, ok := received.Deadline()

					want, _ := ctx.Deadline()
					if !ok || !deadline.Equal(want) {
						t.Fatal("Version lost the stream deadline")
					}

					return version, nil
				}},
			}

			stream := &connectStream{ctx: ctx, send: func() error { cancel(); return nil }}
			if err := service.Connect(&wirev1.ConnectRequest{}, stream); err != nil {
				t.Fatal(err)
			}

			if calls != 1 || stream.response.RuntimeVersion == nil || string(stream.response.GetRuntimeVersion()) != string(version) || stream.response.GetRuntimeIdentity().GetVersion() != "host-7.1" {
				t.Fatalf("Connect calls=%d response=%v", calls, stream.response)
			}

			if _, err := connections.Get(core.ConnectionID(stream.response.GetConnectionId().GetValue())); err == nil {
				t.Fatal("ended handshake retained its connection")
			}
		})
	}
}

func TestConnectMetadataFailureReclaimsConnectionCapacity(t *testing.T) {
	for _, mode := range []string{"error", "panic", "canceled", "deadline", "send failure", "canceled successful retrieval", "canceled private error"} {
		t.Run(mode, func(t *testing.T) {
			lifetime, finish := context.WithTimeout(t.Context(), 10*time.Second)
			defer finish()
			ctx, cancel := context.WithCancel(lifetime)
			defer cancel()
			connections := core.NewConnectionRegistry(1, core.ResourceLimits{})
			service := &RuntimeService{connections: connections, runtime: &versionRuntime{version: func(context.Context) (api.Version, error) {
				switch mode {
				case "panic":
					panic("private version panic")
				case "canceled":
					return "", context.Canceled
				case "deadline":
					return "", context.DeadlineExceeded
				case "send failure":
					return "", nil
				case "canceled private error":
					cancel()

					return "", errors.New("private canceled version")
				case "canceled successful retrieval":
					cancel()

					return "", nil
				default:
					return "", errors.New("private version error")
				}
			}}}
			stream := &connectStream{ctx: ctx, send: func() error { return status.Error(codes.Unavailable, "send failure") }}
			err := service.Connect(&wirev1.ConnectRequest{}, stream)
			want := codes.Internal
			switch mode {
			case "canceled", "canceled successful retrieval", "canceled private error":
				want = codes.Canceled
			case "deadline":
				want = codes.DeadlineExceeded
			case "send failure":
				want = codes.Unavailable
			}

			if status.Code(err) != want || strings.Contains(err.Error(), "private") || (mode != "send failure" && stream.response != nil) {
				t.Fatalf("Connect response=%v error=%v", stream.response, err)
			}

			connection, err := connections.Open()
			if err != nil {
				t.Fatalf("failed handshake retained capacity: %v", err)
			}

			if err := connections.CloseConnection(lifetime, connection.ID()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConnectCancellationReachesHostedVersion(t *testing.T) {
	for _, mode := range []string{"caller", "deadline", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			lifetime, finish := context.WithTimeout(t.Context(), 10*time.Second)
			defer finish()
			ctx, cancel := context.WithCancel(lifetime)
			defer cancel()

			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}

			entered := make(chan struct{})
			connections := core.NewConnectionRegistry(1, core.ResourceLimits{})
			service := &RuntimeService{connections: connections, runtime: &versionRuntime{version: func(ctx context.Context) (api.Version, error) {
				close(entered)
				<-ctx.Done()

				return "", ctx.Err()
			}}}
			stream := &connectStream{ctx: ctx, send: func() error { return nil }}
			finished := make(chan error, 1)
			go func() { finished <- service.Connect(&wirev1.ConnectRequest{}, stream) }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("Version was not entered")
			}

			switch mode {
			case "caller":
				cancel()
			case "shutdown":
				if err := connections.Close(lifetime); err != nil {
					t.Fatal(err)
				}
			}

			select {
			case err := <-finished:
				want := codes.Canceled

				if mode == "deadline" {
					want = codes.DeadlineExceeded
				}

				if status.Code(err) != want || stream.response != nil {
					t.Fatalf("Connect published after cancellation: %v, %v", stream.response, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Connect did not settle")
			}

			if mode != "shutdown" {
				connection, err := connections.Open()
				if err != nil {
					t.Fatalf("cancellation retained capacity: %v", err)
				}

				if err := connections.CloseConnection(lifetime, connection.ID()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
