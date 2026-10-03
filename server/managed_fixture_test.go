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
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server/internal/core"
)

type (
	managedRuntime struct {
		api.Runtime
		version       func(context.Context) (api.Version, error)
		run           func(context.Context) (*api.Output, error)
		compile       func(context.Context) (api.Plan, error)
		plan          *managedPlan
		shutdownError error
		closes        atomic.Int64
		calls         atomic.Int64
	}
	managedPlan struct {
		api.Plan
		close  func() error
		closes atomic.Int64
	}
	readyListener struct {
		net.Listener
		ready chan struct{}
		once  sync.Once
	}
	managedServing struct {
		server   *Server
		listener net.Listener
		cancel   context.CancelFunc
		result   chan error
	}
)

func (l *readyListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.ready) })

	return l.Listener.Accept()
}

func (r *managedRuntime) Version(ctx context.Context) (api.Version, error) {
	r.calls.Add(1)

	if r.version != nil {
		return r.version(ctx)
	}

	return api.Version("managed-test"), ctx.Err()
}
func (r *managedRuntime) Run(ctx context.Context, _ api.Source, _ ...api.SessionOption) (*api.Output, error) {
	r.calls.Add(1)

	if r.run != nil {
		return r.run(ctx)
	}

	return &api.Output{ContentType: "application/json", Content: []byte("[1]")}, ctx.Err()
}
func (r *managedRuntime) Compile(ctx context.Context, _ api.Source, _ ...api.PlanOption) (api.Plan, error) {
	r.calls.Add(1)

	if r.compile != nil {
		return r.compile(ctx)
	}

	return r.plan, nil
}
func (r *managedRuntime) CompileDebug(ctx context.Context, source api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.Compile(ctx, source, options...)
}
func (r *managedRuntime) Close() error                              { r.closes.Add(1); return nil }
func (p *managedPlan) Params(ctx context.Context) ([]string, error) { return nil, ctx.Err() }
func (p *managedPlan) Close() error {
	p.closes.Add(1)

	if p.close != nil {
		return p.close()
	}

	return nil
}
func managedContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	return ctx
}
func managedAwait[T any](t testing.TB, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-managedContext(t).Done():
		t.Fatal("coordinated operation timed out")
		var zero T

		return zero
	}
}
func newManagedServer(t testing.TB, runtime *managedRuntime, options ...Option) *Server {
	t.Helper()

	s, err := New(runtime, options...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := s.Shutdown(managedContext(t)); err != nil && !errors.Is(err, runtime.shutdownError) {
			t.Errorf("shutdown: %v", err)
		}
	})

	return s
}
func startManaged(t testing.TB, s *Server, options ...RunOption) *managedServing {
	t.Helper()
	bound := make(chan net.Listener, 1)
	ready := make(chan struct{})
	s.listen = func(ctx context.Context, network, address string) (net.Listener, error) {
		listener, err := new(net.ListenConfig).Listen(ctx, network, address)
		if err == nil {
			listener = &readyListener{Listener: listener, ready: ready}
			bound <- listener
		}

		return listener, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx, "127.0.0.1:0", options...) }()
	listener := managedAwait(t, bound)
	managedAwait(t, ready)

	return &managedServing{server: s, listener: listener, cancel: cancel, result: result}
}
func managedClient(t testing.TB, listener net.Listener, options ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	if len(options) == 0 {
		options = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	connection, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), options...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close transport: %v", err)
		}
	})

	return connection
}
func managedRemote(t testing.TB, listener net.Listener) api.Runtime {
	t.Helper()

	remote, err := client.New(managedContext(t), managedClient(t, listener))
	if err != nil {
		t.Fatal(err)
	}

	return remote
}
func attachManagedPlan(t testing.TB, s *Server, runtime *managedRuntime) *core.Connection {
	t.Helper()

	connection, err := s.connections.Open()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := core.CompilePlan(managedContext(t), runtime, connection.Resources(), api.NewAnonymousSource("RETURN 1"), false); err != nil {
		t.Fatal(err)
	}

	return connection
}
