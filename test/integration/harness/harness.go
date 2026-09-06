// Package harness hosts observable Universal API doubles behind the public Wire
// server and a real in-process gRPC transport. It is test infrastructure only.
package harness

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type (
	Option func(*configuration)

	configuration struct {
		runtime       api.Runtime
		behavior      RuntimeBehavior
		serverOptions []server.Option
		unavailable   bool
	}

	// Harness owns its listener and transport. Wire borrows the hosted Runtime.
	Harness struct {
		t               testing.TB
		ctx             context.Context
		server          *server.Server
		listener        *bufconn.Listener
		connection      *grpc.ClientConn
		faults          *Faults
		spy             *RuntimeSpy
		runtime         api.Runtime
		serveResult     chan error
		mu              sync.Mutex
		runtimes        []api.Runtime
		expected        []error
		transportClosed bool
		stopped         bool
	}
)

func WithBehavior(behavior RuntimeBehavior) Option {
	return func(c *configuration) { c.behavior = behavior }
}

func WithRuntime(runtime api.Runtime) Option {
	return func(c *configuration) { c.runtime = runtime }
}

func WithServerOptions(options ...server.Option) Option {
	return func(c *configuration) { c.serverOptions = append(c.serverOptions, options...) }
}

// WithUnavailableServer leaves a closed listener for handshake failure tests.
func WithUnavailableServer() Option {
	return func(c *configuration) { c.unavailable = true }
}

func New(t testing.TB, options ...Option) *Harness {
	t.Helper()
	var configured configuration

	for _, option := range options {
		option(&configured)
	}

	h := &Harness{t: t, ctx: Context(t)}
	t.Cleanup(h.cleanup)
	hosted := configured.runtime
	if hosted == nil {
		h.spy = NewRuntimeSpy(configured.behavior)
		hosted = h.spy
	} else {
		h.spy, _ = hosted.(*RuntimeSpy)
	}

	var err error
	h.server, err = server.NewServer(hosted, configured.serverOptions...)
	if err != nil {
		t.Fatal(err)
	}

	h.listener = bufconn.Listen(1 << 20)

	if configured.unavailable {
		if err := h.listener.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		h.serveResult = make(chan error, 1)
		go func() { h.serveResult <- h.server.Serve(context.Background(), h.listener) }()
	}

	h.connection, err = grpc.NewClient(
		"passthrough:///wire-integration",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return h.listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	h.faults = newFaults(h.connection)

	if !configured.unavailable {
		h.runtime, err = h.OpenRuntime()
		if err != nil {
			t.Fatal(err)
		}
	}

	return h
}

func (h *Harness) Runtime() api.Runtime {
	return h.runtime
}

func (h *Harness) RuntimeSpy() *RuntimeSpy {
	return h.spy
}

func (h *Harness) Context() context.Context {
	return h.ctx
}

func (h *Harness) Faults() *Faults {
	return h.faults
}

func (h *Harness) OpenRuntime() (api.Runtime, error) {
	runtime, err := client.New(h.ctx, h.faults)
	if err != nil {
		return nil, err
	}

	h.mu.Lock()
	h.runtimes = append(h.runtimes, runtime)
	h.mu.Unlock()

	return runtime, nil
}

func (h *Harness) ExpectCleanupError(err error) {
	h.mu.Lock()
	h.expected = append(h.expected, err)
	h.mu.Unlock()
}

func (h *Harness) Shutdown() error {
	h.mu.Lock()
	h.stopped = true
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return h.server.Shutdown(ctx)
}

func (h *Harness) CloseTransport() error {
	h.mu.Lock()

	if h.transportClosed {
		h.mu.Unlock()

		return nil
	}

	h.transportClosed = true
	h.mu.Unlock()

	return h.connection.Close()
}

func (h *Harness) expectedError(err error) bool {
	if err == nil {
		return true
	}

	// A matching expected error must not hide another failure in errors.Join.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if !h.expectedError(cause) {
				return false
			}
		}

		return true
	}

	if cause := errors.Unwrap(err); cause != nil {
		return h.expectedError(cause)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, expected := range h.expected {
		if errors.Is(err, expected) {
			return true
		}
	}

	if h.transportClosed || h.stopped {
		return errors.Is(err, client.ErrClosed) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.Canceled
	}

	return false
}

func (h *Harness) cleanup() {
	if h.faults != nil {
		h.faults.reset()
	}

	for i := len(h.runtimes) - 1; i >= 0; i-- {
		if err := h.runtimes[i].Close(); !h.expectedError(err) {
			h.t.Errorf("close logical Runtime: %v", err)
		}
	}

	// Assert logical-client reclamation before server shutdown could hide a leak.
	if h.spy != nil {
		h.spy.Recorder().AssertClosed(h.t)
	}

	if h.server != nil {
		if err := h.Shutdown(); !h.expectedError(err) {
			h.t.Errorf("shutdown Wire server: %v", err)
		}
	}

	if h.connection != nil {
		if err := h.CloseTransport(); err != nil {
			h.t.Errorf("close transport: %v", err)
		}
	}

	if h.listener != nil {
		if err := h.listener.Close(); err != nil {
			h.t.Errorf("close listener: %v", err)
		}
	}

	if h.serveResult != nil {
		if err := Await(h.t, h.serveResult); err != nil {
			h.t.Errorf("serve: %v", err)
		}
	}

	if h.spy != nil {
		h.spy.Recorder().AssertClosed(h.t)
	}
}
