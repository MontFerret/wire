package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/test/securityfixture"
)

type (
	countedListener struct {
		net.Listener
		closes atomic.Int64
	}
	failedListener struct {
		net.Listener
		entered, release, closed chan struct{}
		once                     sync.Once
		failure                  error
	}
	observingCredentials struct {
		credentials.TransportCredentials
		entered chan struct{}
		once    *sync.Once
	}
)

func (l *countedListener) Close() error { l.closes.Add(1); return l.Listener.Close() }
func (l *failedListener) Accept() (net.Conn, error) {
	close(l.entered)
	<-l.release

	return nil, l.failure
}
func (l *failedListener) Close() error {
	l.once.Do(func() { close(l.closed) })

	return l.Listener.Close()
}

func TestRunValidationDoesNotListenOrConsumeServer(t *testing.T) {
	var zero RunOption
	tests := []struct {
		name, address string
		ctx           context.Context
		options       []RunOption
	}{
		{name: "empty", ctx: context.Background()},
		{name: "nil option", address: "127.0.0.1:0", ctx: context.Background(), options: []RunOption{nil}},
		{name: "zero value option", address: "127.0.0.1:0", ctx: context.Background(), options: []RunOption{zero}},
		{name: "zero timeout", address: "127.0.0.1:0", ctx: context.Background(), options: []RunOption{WithShutdownTimeout(0)}},
		{name: "negative timeout", address: "127.0.0.1:0", ctx: context.Background(), options: []RunOption{WithShutdownTimeout(-time.Second)}},
		{name: "nil context", address: "127.0.0.1:0"},
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests = append(tests, struct {
		name, address string
		ctx           context.Context
		options       []RunOption
	}{name: "cancelled", address: "127.0.0.1:0", ctx: cancelled})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newManagedServer(t, &managedRuntime{})
			var calls atomic.Int64
			s.listen = func(context.Context, string, string) (net.Listener, error) {
				calls.Add(1)

				return nil, errors.New("unexpected listen")
			}

			err := s.Run(test.ctx, test.address, test.options...)
			if err == nil || calls.Load() != 0 {
				t.Fatalf("validation err=%v listen calls=%d", err, calls.Load())
			}

			if test.name == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}

			running := startManaged(t, s)

			remote := managedRemote(t, running.listener)
			if err := remote.Close(); err != nil {
				t.Fatal(err)
			}

			running.cancel()

			if err := managedAwait(t, running.result); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestRunBindFailureAllowsRetry(t *testing.T) {
	s := newManagedServer(t, &managedRuntime{})
	if err := s.Run(managedContext(t), "bad address"); err == nil {
		t.Fatal("invalid address accepted")
	}

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := occupied.Close(); err != nil {
			t.Error(err)
		}
	}()

	if err := s.Run(managedContext(t), occupied.Addr().String()); err == nil {
		t.Fatal("occupied address accepted")
	}

	running := startManaged(t, s)

	remote := managedRemote(t, running.listener)
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	running.cancel()

	if err := managedAwait(t, running.result); err != nil {
		t.Fatal(err)
	}

	if _, err := running.listener.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("listener retained: %v", err)
	}

	rebound, err := net.Listen("tcp", running.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	if err := rebound.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Run(managedContext(t), "127.0.0.1:0"); err == nil {
		t.Fatal("second serving attempt accepted")
	}
}
func TestRunCompetingStartsLeaveAcceptedInvocationOperational(t *testing.T) {
	for _, competitor := range []string{"run", "serve"} {
		t.Run(competitor, func(t *testing.T) {
			s := newManagedServer(t, &managedRuntime{})
			entered, release := make(chan struct{}), make(chan struct{})
			var listens atomic.Int64
			bound := make(chan net.Listener, 1)
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			s.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
				listens.Add(1)
				close(entered)
				<-release

				listener, err := new(net.ListenConfig).Listen(ctx, network, address)
				if err == nil {
					bound <- listener
				}

				return listener, err
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.Run(ctx, "127.0.0.1:0") }()
			managedAwait(t, entered)

			if competitor == "run" {
				if err := s.Run(managedContext(t), "127.0.0.1:0"); err == nil {
					t.Fatal("competing Run accepted")
				}
			} else {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}

				counted := &countedListener{Listener: listener}
				if err := s.Serve(managedContext(t), counted); err == nil {
					t.Fatal("competing Serve accepted")
				}

				if counted.closes.Load() != 0 {
					t.Fatal("rejected listener closed")
				}

				if err := counted.Close(); err != nil {
					t.Fatal(err)
				}
			}

			if listens.Load() != 1 {
				t.Fatal("rejected Run opened a listener")
			}

			releaseOnce.Do(func() { close(release) })

			remote := managedRemote(t, managedAwait(t, bound))
			if _, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1")); err != nil {
				t.Fatal(err)
			}

			if err := remote.Close(); err != nil {
				t.Fatal(err)
			}

			cancel()

			if err := managedAwait(t, result); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}

	// Competing starts after serving commitment must also leave real RPCs usable.
	s := newManagedServer(t, &managedRuntime{})
	running := startManaged(t, s)

	remote := managedRemote(t, running.listener)
	if err := s.Run(managedContext(t), "127.0.0.1:0"); err == nil {
		t.Fatal("active start accepted")
	}

	if _, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1")); err != nil {
		t.Fatal(err)
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	running.cancel()

	if err := managedAwait(t, running.result); err != nil {
		t.Fatal(err)
	}
}
func TestRunShutdownBeforeAndDuringStartup(t *testing.T) {
	for _, bindFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "late listener", true: "late bind failure"}[bindFailure], func(t *testing.T) {
			s := newManagedServer(t, &managedRuntime{})
			entered, release := make(chan struct{}), make(chan struct{})
			failure := errors.New("bind failed")
			var listener net.Listener
			s.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
				var err error

				if !bindFailure {
					listener, err = new(net.ListenConfig).Listen(ctx, network, address)
					if err != nil {
						return nil, err
					}
				}

				close(entered)
				<-release

				if bindFailure {
					return nil, failure
				}

				return listener, nil
			}
			result := make(chan error, 1)
			go func() { result <- s.Run(context.Background(), "127.0.0.1:0") }()
			managedAwait(t, entered)

			if err := s.Shutdown(managedContext(t)); err != nil {
				t.Fatal(err)
			}

			close(release)

			err := managedAwait(t, result)
			if bindFailure && !errors.Is(err, failure) {
				t.Fatal(err)
			}

			if !bindFailure && !errors.Is(err, grpc.ErrServerStopped) {
				t.Fatal(err)
			}

			if listener != nil {
				if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
					t.Fatal(err)
				}
			}

			if err := s.Run(managedContext(t), "127.0.0.1:0"); !errors.Is(err, grpc.ErrServerStopped) {
				t.Fatal(err)
			}
		})
	}

	s := newManagedServer(t, &managedRuntime{})
	if err := s.Shutdown(managedContext(t)); err != nil {
		t.Fatal(err)
	}

	if err := s.Run(managedContext(t), "127.0.0.1:0"); !errors.Is(err, grpc.ErrServerStopped) {
		t.Fatal(err)
	}
}
func TestRunShutdownRetainsCleanupFailureAndNeverClosesRuntime(t *testing.T) {
	failure := errors.New("host close failure")
	plan := &managedPlan{close: func() error { return failure }}
	runtime := &managedRuntime{plan: plan, shutdownError: failure}
	s := newManagedServer(t, runtime)
	running := startManaged(t, s)
	attachManagedPlan(t, s, runtime)
	running.cancel()

	if err := managedAwait(t, running.result); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	for range 2 {
		if err := s.Shutdown(managedContext(t)); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}

	if plan.closes.Load() != 1 || runtime.closes.Load() != 0 {
		t.Fatalf("plan closes=%d runtime closes=%d", plan.closes.Load(), runtime.closes.Load())
	}
}
func TestRunBudgetBoundsBackgroundShutdownAndRetainsCleanup(t *testing.T) {
	for _, shorten := range []bool{false, true} {
		t.Run(map[bool]string{false: "managed budget", true: "explicit earlier deadline"}[shorten], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			plan := &managedPlan{close: func() error { close(entered); <-release; return nil }}
			runtime := &managedRuntime{plan: plan}
			s := newManagedServer(t, runtime)
			timeout := 50 * time.Millisecond

			if shorten {
				timeout = 5 * time.Second
			}

			running := startManaged(t, s, WithShutdownTimeout(timeout))
			remote := managedRemote(t, running.listener)
			conn := managedClient(t, running.listener)

			stream, err := wirev1.NewRuntimeServiceClient(conn).Connect(managedContext(t), &wirev1.ConnectRequest{})
			if err != nil {
				t.Fatal(err)
			}

			handshake, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := wirev1.NewPlanServiceClient(conn).Compile(managedContext(t), &wirev1.CompileRequest{ConnectionId: handshake.ConnectionId, Source: &wirev1.Source{Content: "RETURN 1"}}); err != nil {
				t.Fatal(err)
			}

			transportResult := make(chan error, 1)
			go func() { _, err := stream.Recv(); transportResult <- err }()
			result := make(chan error, 1)
			go func() { result <- s.Shutdown(context.Background()) }()
			managedAwait(t, entered)

			if shorten {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()

				if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			}

			running.cancel()

			if err := managedAwait(t, running.result); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}

			if err := managedAwait(t, transportResult); status.Code(err) != codes.Unavailable {
				t.Fatalf("forced transport shutdown: %v", err)
			}

			select {
			case err := <-result:
				t.Fatalf("cleanup falsely settled: %v", err)
			default:
			}

			if plan.closes.Load() != 1 {
				t.Fatal("cleanup not committed once")
			}

			if _, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1")); err == nil {
				t.Fatal("transport remained usable")
			}

			releaseOnce.Do(func() { close(release) })

			if err := managedAwait(t, result); err != nil {
				t.Fatal(err)
			}

			if err := s.Shutdown(managedContext(t)); err != nil {
				t.Fatal(err)
			}

			if plan.closes.Load() != 1 || runtime.closes.Load() != 0 {
				t.Fatal("ownership violated")
			}
		})
	}
}
func TestRunServingFailureSurvivesCancellationAndJoinsCleanup(t *testing.T) {
	servingCause, cleanupFailure := errors.New("accept failed"), errors.New("cleanup failed")
	servingFailure := &net.OpError{Op: "accept", Net: "tcp", Err: servingCause}
	runtime := &managedRuntime{plan: &managedPlan{close: func() error { return cleanupFailure }}, shutdownError: cleanupFailure}
	s := newManagedServer(t, runtime)

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	listener := &failedListener{Listener: raw, entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}), failure: servingFailure}
	s.listen = func(context.Context, string, string) (net.Listener, error) { return listener, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx, "127.0.0.1:0") }()
	managedAwait(t, listener.entered)
	attachManagedPlan(t, s, runtime)
	cancel()
	managedAwait(t, listener.closed)
	close(listener.release)

	err = managedAwait(t, result)

	var operationError *net.OpError
	if !errors.Is(err, servingCause) || !errors.Is(err, cleanupFailure) || !errors.As(err, &operationError) {
		t.Fatalf("lost independent failures: %v", err)
	}
}

func TestRunExplicitShutdownAndBudgetStartsAtShutdown(t *testing.T) {
	runtime := &managedRuntime{plan: &managedPlan{}}
	s := newManagedServer(t, runtime)
	running := startManaged(t, s, WithShutdownTimeout(30*time.Millisecond))
	remote := managedRemote(t, running.listener)
	attachManagedPlan(t, s, runtime)
	// Time passing while the host serves must not consume the cleanup budget.
	timer := time.NewTimer(60 * time.Millisecond)
	defer timer.Stop()
	managedAwait(t, timer.C)

	if _, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1")); err != nil {
		t.Fatal(err)
	}

	if err := s.Shutdown(managedContext(t)); err != nil {
		t.Fatal(err)
	}

	if err := managedAwait(t, running.result); err != nil {
		t.Fatal(err)
	}

	if runtime.plan.closes.Load() != 1 || runtime.closes.Load() != 0 {
		t.Fatal("ownership violated")
	}
}
func TestRunRejectedWhileServeRemainsOperational(t *testing.T) {
	s := newManagedServer(t, &managedRuntime{})
	listener := startSecurityServer(t, s, false)

	remote := managedRemote(t, listener)
	if err := s.Run(managedContext(t), "127.0.0.1:0"); err == nil {
		t.Fatal("Run accepted during Serve")
	}

	if _, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1")); err != nil {
		t.Fatal(err)
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestRunCancelledDuringListenAllowsRetry(t *testing.T) {
	s := newManagedServer(t, &managedRuntime{})
	entered := make(chan struct{})
	s.listen = func(ctx context.Context, _ string, _ string) (net.Listener, error) {
		close(entered)
		<-ctx.Done()

		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx, "127.0.0.1:0") }()
	managedAwait(t, entered)
	cancel()

	if err := managedAwait(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	running := startManaged(t, s)

	remote := managedRemote(t, running.listener)
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	running.cancel()

	if err := managedAwait(t, running.result); err != nil {
		t.Fatal(err)
	}
}

func TestRunCancelledAfterBindingClosesListenerAndAllowsRetry(t *testing.T) {
	s := newManagedServer(t, &managedRuntime{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var bound *countedListener
	address := "127.0.0.1:0"
	s.listen = func(startup context.Context, network, supplied string) (net.Listener, error) {
		if network != "tcp" || supplied != address {
			t.Fatalf("listen arguments changed: %q %q", network, supplied)
		}

		listener, err := new(net.ListenConfig).Listen(startup, network, supplied)
		if err != nil {
			return nil, err
		}

		bound = &countedListener{Listener: listener}
		cancel()

		return bound, nil
	}

	if err := s.Run(ctx, address); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	if bound.closes.Load() != 1 {
		t.Fatal("pre-serving listener was not released exactly once")
	}

	running := startManaged(t, s)

	remote := managedRemote(t, running.listener)
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	running.cancel()

	if err := managedAwait(t, running.result); err != nil {
		t.Fatal(err)
	}
}

func TestRunServingDeadlineDoesNotBecomeCleanupDeadline(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	runtime := &managedRuntime{plan: &managedPlan{close: func() error { close(entered); <-release; return nil }}}
	s := newManagedServer(t, runtime)
	bound := make(chan net.Listener, 1)
	s.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		listener, err := new(net.ListenConfig).Listen(ctx, network, address)
		if err == nil {
			bound <- listener
		}

		return listener, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx, "127.0.0.1:0", WithShutdownTimeout(time.Second)) }()
	remote := managedRemote(t, managedAwait(t, bound))
	attachManagedPlan(t, s, runtime)
	managedAwait(t, entered)
	once.Do(func() { close(release) })

	if err := managedAwait(t, result); err != nil {
		t.Fatal(err)
	}

	// Shutdown owns the remaining logical handle after its serving deadline.
	if err := remote.Close(); err != nil && !errors.Is(err, client.ErrClosed) && status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}

func (c *observingCredentials) ServerHandshake(connection net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c.once.Do(func() { close(c.entered) })

	return c.TransportCredentials.ServerHandshake(connection)
}
func (c *observingCredentials) Clone() credentials.TransportCredentials {
	return &observingCredentials{TransportCredentials: c.TransportCredentials.Clone(), entered: c.entered, once: c.once}
}
func TestRunBudgetDoesNotWaitForBlockedTLSHandshake(t *testing.T) {
	certs := securityfixture.NewCertificates(t)
	entered := make(chan struct{})
	creds := &observingCredentials{TransportCredentials: credentials.NewTLS(certs.ServerConfig(false)), entered: entered, once: &sync.Once{}}
	s := newManagedServer(t, &managedRuntime{}, WithTransportCredentials(creds))
	running := startManaged(t, s, WithShutdownTimeout(50*time.Millisecond))

	connection, err := new(net.Dialer).DialContext(managedContext(t), "tcp", running.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	}()
	managedAwait(t, entered)
	running.cancel()

	if err := managedAwait(t, running.result); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("managed wait blocked on handshake: %v", err)
	}

	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.Shutdown(managedContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestRunTimeoutPreservesCompletedCleanupFailure(t *testing.T) {
	failure := errors.New("completed hosted cleanup failed")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	runtime := &managedRuntime{plan: &managedPlan{close: func() error { return failure }}, shutdownError: failure}
	s := newManagedServer(t, runtime, WithStreamInterceptors(func(host any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		err := next(host, stream)
		close(entered)
		<-release

		return err
	}))
	running := startManaged(t, s, WithShutdownTimeout(50*time.Millisecond))
	conn := managedClient(t, running.listener)

	stream, err := wirev1.NewRuntimeServiceClient(conn).Connect(managedContext(t), &wirev1.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}

	handshake, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := wirev1.NewPlanServiceClient(conn).Compile(managedContext(t), &wirev1.CompileRequest{ConnectionId: handshake.ConnectionId, Source: &wirev1.Source{Content: "RETURN 1"}}); err != nil {
		t.Fatal(err)
	}

	running.cancel()
	managedAwait(t, entered)

	if err := managedAwait(t, running.result); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, failure) {
		t.Fatalf("known cleanup failure lost on timeout: %v", err)
	}

	once.Do(func() { close(release) })

	if err := s.Shutdown(managedContext(t)); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	if runtime.plan.closes.Load() != 1 {
		t.Fatal("cleanup repeated")
	}
}

func TestServeDoesNotAcquireManagedShutdownBudget(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	runtime := &managedRuntime{plan: &managedPlan{close: func() error { close(entered); <-release; return nil }}}
	s := newManagedServer(t, runtime)
	listener := startSecurityServer(t, s, false)
	remote := managedRemote(t, listener)
	attachManagedPlan(t, s, runtime)
	result := make(chan error, 1)
	go func() { result <- s.Shutdown(context.Background()) }()
	managedAwait(t, entered)
	s.serveMu.Lock()
	deadline := s.shutdownDeadline
	s.serveMu.Unlock()

	if !deadline.IsZero() {
		t.Fatalf("Serve inherited a default timeout: %v", deadline)
	}

	once.Do(func() { close(release) })

	if err := managedAwait(t, result); err != nil {
		t.Fatal(err)
	}

	if err := remote.Close(); err != nil && !errors.Is(err, client.ErrClosed) && status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}
