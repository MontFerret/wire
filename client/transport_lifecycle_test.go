package client

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

type (
	constructorConnection struct {
		newStream       func(context.Context) (grpc.ClientStream, error)
		release         func(context.Context, *wirev1.CloseConnectionRequest) error
		streams         atomic.Int64
		releases        atomic.Int64
		transportCloses atomic.Int64
	}
	constructorStream struct {
		grpc.ClientStream
		ctx      context.Context
		first    func(*wirev1.ConnectResponse) error
		terminal <-chan error
		received atomic.Int64
	}
)

func (c *constructorConnection) NewStream(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	c.streams.Add(1)

	if c.newStream != nil {
		return c.newStream(ctx)
	}

	return &constructorStream{ctx: ctx}, nil
}

func (c *constructorConnection) Invoke(ctx context.Context, _ string, request, _ any, _ ...grpc.CallOption) error {
	c.releases.Add(1)

	if c.release != nil {
		return c.release(ctx, request.(*wirev1.CloseConnectionRequest))
	}

	return nil
}

func (c *constructorConnection) Close() error {
	c.transportCloses.Add(1)

	return nil
}

func (s *constructorStream) Context() context.Context { return s.ctx }
func (*constructorStream) SendMsg(any) error          { return nil }
func (*constructorStream) CloseSend() error           { return nil }
func (s *constructorStream) RecvMsg(message any) error {
	if s.received.Add(1) == 1 {
		out := message.(*wirev1.ConnectResponse)

		if s.first != nil {
			return s.first(out)
		}

		out.ConnectionId = &wirev1.ConnectionId{Value: "known-connection"}
		out.Protocol = &wirev1.ProtocolInfo{Name: "ferret.wire", Version: "v1"}
		out.RuntimeVersion = []byte("version")

		return nil
	}

	select {
	case err := <-s.terminal:
		return err
	case <-s.ctx.Done():
		return status.FromContextError(s.ctx.Err()).Err()
	}
}

func TestFromInvalidInputsPreventDispatch(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	var typedNil *constructorConnection
	connection := &constructorConnection{}
	for _, test := range []struct {
		name string
		ctx  context.Context
		conn grpc.ClientConnInterface
	}{
		{name: "nil context", conn: connection},
		{name: "cancelled context", ctx: cancelled, conn: connection},
		{name: "nil connection", ctx: t.Context()},
		{name: "typed nil connection", ctx: t.Context(), conn: typedNil},
	} {
		t.Run(test.name, func(t *testing.T) {
			remote, err := From(test.ctx, test.conn)
			if err == nil || remote != nil {
				t.Fatalf("invalid From returned %v, %v", remote, err)
			}
		})
	}

	if connection.streams.Load() != 0 || connection.transportCloses.Load() != 0 {
		t.Fatal("validation dispatched or closed the borrowed transport")
	}
}

func TestOwnedStartupCancellationCoversCreationAndReceive(t *testing.T) {
	for _, stage := range []string{"creation", "receive"} {
		t.Run(stage, func(t *testing.T) {
			entered := make(chan struct{})
			ctx, cancel := context.WithCancel(testClientContext(t))
			defer cancel()
			connection := &constructorConnection{newStream: func(streamCtx context.Context) (grpc.ClientStream, error) {
				if stage == "creation" {
					close(entered)
					<-streamCtx.Done()

					return nil, status.FromContextError(streamCtx.Err()).Err()
				}

				return &constructorStream{ctx: streamCtx, first: func(*wirev1.ConnectResponse) error {
					close(entered)
					<-streamCtx.Done()

					return status.FromContextError(streamCtx.Err()).Err()
				}}, nil
			}}
			result := make(chan error, 1)
			go func() {
				remote, err := newRuntime(ctx, connection, connection.Close)
				if remote != nil {
					err = errors.Join(err, errors.New("failed startup published runtime"))
				}

				result <- err
			}()
			awaitConstructor(t, entered)
			cancel()

			if err := awaitConstructor(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("startup lost cancellation: %v", err)
			}

			if connection.transportCloses.Load() != 1 || connection.releases.Load() != 0 {
				t.Fatalf("unknown allocation rollback: transport=%d logical=%d", connection.transportCloses.Load(), connection.releases.Load())
			}
		})
	}
}

func TestConstructorRollbackRetainsKnownAllocationErrorsAndMetadata(t *testing.T) {
	type contextKey struct{}
	logicalErr, transportErr := errors.New("logical cleanup"), errors.New("channel cleanup")
	for _, mode := range []string{"borrowed", "owned"} {
		for _, failure := range []string{"malformed", "cancellation"} {
			t.Run(mode+"/"+failure, func(t *testing.T) {
				ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(context.WithValue(testClientContext(t), contextKey{}, "value"), "startup", "retained"))
				defer cancel()
				connection := &constructorConnection{newStream: func(streamCtx context.Context) (grpc.ClientStream, error) {
					return &constructorStream{ctx: streamCtx, first: func(out *wirev1.ConnectResponse) error {
						out.ConnectionId = &wirev1.ConnectionId{Value: "known"}

						if failure == "cancellation" {
							out.Protocol = &wirev1.ProtocolInfo{Name: "wire", Version: "v1"}
							out.RuntimeVersion = []byte{}
							cancel()
						}

						return nil
					}}, nil
				}, release: func(cleanup context.Context, request *wirev1.CloseConnectionRequest) error {
					md, _ := metadata.FromOutgoingContext(cleanup)
					if cleanup.Err() != nil || cleanup.Value(contextKey{}) != "value" || len(md.Get("startup")) != 1 || request.GetConnectionId().GetValue() != "known" {
						t.Error("rollback lost values, metadata, or known allocation")
					}

					return logicalErr
				}}
				var closeTransport func() error

				if mode == "owned" {
					closeTransport = func() error {
						connection.transportCloses.Add(1)

						return transportErr
					}
				}

				remote, err := newRuntime(ctx, connection, closeTransport)
				if remote != nil || !errors.Is(err, logicalErr) || mode == "owned" && !errors.Is(err, transportErr) {
					t.Fatalf("rollback dropped an error or published runtime: %v, %v", remote, err)
				}

				if failure == "cancellation" && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost startup cancellation: %v", err)
				}

				want := int64(0)

				if mode == "owned" {
					want = 1
				}

				if connection.releases.Load() != 1 || connection.transportCloses.Load() != want {
					t.Fatalf("rollback counts: logical=%d transport=%d", connection.releases.Load(), connection.transportCloses.Load())
				}
			})
		}
	}
}

func TestConnectionTeardownJoinsFailuresExactlyOnce(t *testing.T) {
	logicalErr, transportErr := errors.New("release failed"), errors.New("channel failed")
	connection := &constructorConnection{release: func(context.Context, *wirev1.CloseConnectionRequest) error { return logicalErr }}

	remote, err := newRuntime(testClientContext(t), connection, func() error {
		connection.transportCloses.Add(1)

		return transportErr
	})
	if err != nil {
		t.Fatal(err)
	}

	var callers sync.WaitGroup
	results := make(chan error, 20)
	for range 20 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			results <- remote.Close()
		}()
	}

	callers.Wait()
	for range 20 {
		err := awaitConstructor(t, results)
		if !errors.Is(err, logicalErr) || !errors.Is(err, transportErr) {
			t.Fatalf("close lost independent errors: %v", err)
		}
	}

	if connection.releases.Load() != 1 || connection.transportCloses.Load() != 1 {
		t.Fatalf("teardown repeated: %d, %d", connection.releases.Load(), connection.transportCloses.Load())
	}
}

func TestConnectionCloseWaiterTimeoutDoesNotAbandonTransport(t *testing.T) {
	entered, allow := make(chan struct{}), make(chan struct{})
	logicalErr := errors.New("late cleanup failure")
	connection := &constructorConnection{release: func(context.Context, *wirev1.CloseConnectionRequest) error {
		close(entered)
		<-allow

		return logicalErr
	}}

	handle, err := newConnection(testClientContext(t), connection, connection.Close)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- handle.Close(ctx) }()
	awaitConstructor(t, entered)

	if err := awaitConstructor(t, first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter did not time out: %v", err)
	}

	if connection.transportCloses.Load() != 0 {
		t.Fatal("channel closed before logical cleanup attempt settled")
	}

	close(allow)

	if err := handle.Close(testClientContext(t)); !errors.Is(err, logicalErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late cleanup lost retained error: %v", err)
	}

	if connection.releases.Load() != 1 || connection.transportCloses.Load() != 1 {
		t.Fatal("expired waiter abandoned or repeated teardown")
	}
}

func TestTerminalConnectLossReleasesOwnedTransportAndRetainsCause(t *testing.T) {
	for _, terminalErr := range []error{io.EOF, status.Error(codes.Unavailable, "connection lost")} {
		connection := &constructorConnection{}
		terminal := make(chan error, 1)
		connection.newStream = func(ctx context.Context) (grpc.ClientStream, error) {
			return &constructorStream{ctx: ctx, terminal: terminal}, nil
		}

		handle, err := newConnection(testClientContext(t), connection, connection.Close)
		if err != nil {
			t.Fatal(err)
		}

		terminal <- terminalErr
		awaitConstructor(t, handle.closeDone)

		err = handle.checkOpen()
		if terminalErr == io.EOF && !errors.Is(err, ErrClosed) || terminalErr != io.EOF && status.Code(err) != codes.Unavailable { //nolint:errorlint // Distinguish the exact EOF fixture from the injected transport status.
			t.Fatalf("terminal cause changed: %v", err)
		}

		if err := handle.Close(testClientContext(t)); err != nil {
			t.Fatal(err)
		}

		if connection.transportCloses.Load() != 1 || connection.releases.Load() != 1 {
			t.Fatal("terminal path bypassed or repeated owned teardown")
		}
	}
}

func TestLostAllocationRecoveryRespectsTransportOwnership(t *testing.T) {
	for _, owned := range []bool{false, true} {
		connection := &constructorConnection{}
		var closeTransport func() error

		if owned {
			closeTransport = connection.Close
		}

		handle, err := newConnection(testClientContext(t), connection, closeTransport)
		if err != nil {
			t.Fatal(err)
		}

		startupErr := status.Error(codes.Unavailable, "lost allocation")

		err = handle.reclaimAllocation(testClientContext(t), &allocationError{cause: startupErr})
		if !errors.Is(err, startupErr) {
			t.Fatalf("recovery lost allocation cause: %v", err)
		}

		want := int64(0)

		if owned {
			want = 1
		}

		if connection.transportCloses.Load() != want || connection.releases.Load() != 1 {
			t.Fatal("recovery used wrong transport ownership")
		}
	}
}

func awaitConstructor[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-testClientContext(t).Done():
		t.Fatal("constructor test coordination timed out")

		var zero T

		return zero
	}
}

func TestConstructorPublicationCancellationRace(t *testing.T) {
	for _, owned := range []bool{false, true} {
		for range 50 {
			entered, allow := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(testClientContext(t))
			connection := &constructorConnection{newStream: func(streamCtx context.Context) (grpc.ClientStream, error) {
				return &constructorStream{ctx: streamCtx, first: func(out *wirev1.ConnectResponse) error {
					close(entered)
					<-allow
					out.ConnectionId = &wirev1.ConnectionId{Value: "known"}
					out.Protocol = &wirev1.ProtocolInfo{Name: "wire", Version: "v1"}
					out.RuntimeVersion = []byte{}

					return nil
				}}, nil
			}}
			var closer func() error

			if owned {
				closer = connection.Close
			}

			done := make(chan error, 1)
			go func() {
				remote, err := newRuntime(ctx, connection, closer)
				if remote != nil {
					if err != nil {
						done <- errors.New("failure published a runtime")

						return
					}

					err = remote.Close()
				} else if !errors.Is(err, context.Canceled) {
					err = errors.New("cancelled publication lost context error")
				} else {
					err = nil
				}

				done <- err
			}()
			awaitConstructor(t, entered)
			cancelled := make(chan struct{})
			go func() { cancel(); close(cancelled) }()
			close(allow)
			awaitConstructor(t, cancelled)

			if err := awaitConstructor(t, done); err != nil {
				t.Fatal(err)
			}

			want := int64(0)

			if owned {
				want = 1
			}

			if connection.releases.Load() != 1 || connection.transportCloses.Load() != want {
				t.Fatal("publication race leaked or duplicated teardown")
			}
		}
	}
}

func TestOwnedTeardownContainsCleanupPanics(t *testing.T) {
	for _, kind := range []string{"logical", "transport"} {
		connection := &constructorConnection{}

		if kind == "logical" {
			connection.release = func(context.Context, *wirev1.CloseConnectionRequest) error { panic("private detail") }
		}

		remote, err := newRuntime(testClientContext(t), connection, func() error {
			connection.transportCloses.Add(1)

			if kind == "transport" {
				panic("private detail")
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := remote.Close(); err == nil || strings.Contains(err.Error(), "private detail") {
			t.Fatalf("cleanup panic was lost or exposed: %v", err)
		}

		if connection.transportCloses.Load() != 1 || connection.releases.Load() != 1 {
			t.Fatal("cleanup panic bypassed teardown")
		}
	}
}

func TestStartupDeadlineRollsBackBothOwnershipModes(t *testing.T) {
	for _, owned := range []bool{false, true} {
		for _, stage := range []string{"creation", "receive"} {
			connection := &constructorConnection{newStream: func(streamCtx context.Context) (grpc.ClientStream, error) {
				if stage == "creation" {
					<-streamCtx.Done()

					return nil, status.FromContextError(streamCtx.Err()).Err()
				}

				return &constructorStream{ctx: streamCtx, first: func(*wirev1.ConnectResponse) error {
					<-streamCtx.Done()

					return status.FromContextError(streamCtx.Err()).Err()
				}}, nil
			}}
			var closer func() error

			if owned {
				closer = connection.Close
			}

			ctx, cancel := context.WithTimeout(testClientContext(t), 25*time.Millisecond)
			remote, err := newRuntime(ctx, connection, closer)
			cancel()

			if remote != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("startup deadline did not reclaim runtime: %v, %v", remote, err)
			}

			want := int64(0)

			if owned {
				want = 1
			}

			if connection.transportCloses.Load() != want {
				t.Fatal("startup timeout used wrong transport ownership")
			}

			// The same borrowed transport remains independently usable.
			if !owned {
				connection.newStream = nil

				other, err := From(testClientContext(t), connection)
				if err != nil {
					t.Fatal(err)
				}

				if err := other.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestLogicalCleanupDeadlineStillClosesOwnedChannel(t *testing.T) {
	transportErr := errors.New("channel close failed")
	connection := &constructorConnection{release: func(ctx context.Context, _ *wirev1.CloseConnectionRequest) error {
		<-ctx.Done()

		return ctx.Err()
	}}

	handle, err := newConnection(testClientContext(t), connection, func() error {
		connection.transportCloses.Add(1)

		return transportErr
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(testClientContext(t), 25*time.Millisecond)
	defer cancel()
	_ = handle.Close(ctx)
	awaitConstructor(t, handle.closeDone)

	if err := handle.Close(testClientContext(t)); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, transportErr) {
		t.Fatalf("timed-out cleanup lost an independent failure: %v", err)
	}

	if connection.transportCloses.Load() != 1 || connection.releases.Load() != 1 {
		t.Fatal("timed-out logical cleanup bypassed channel closure")
	}
}
