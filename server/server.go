package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/core"
	"github.com/MontFerret/wire/server/internal/grpcserver"
)

type (
	// Server hosts Wire through Run or a caller-created listener passed to Serve.
	// It borrows the runtime passed to New and never closes it. Serving is single-use.
	Server struct {
		grpcServer  *grpc.Server
		connections *core.ConnectionRegistry
		listen      func(context.Context, string, string) (net.Listener, error)

		serveMu          sync.Mutex
		state            servingState
		startupCancel    context.CancelFunc
		listener         *servingListener
		managedTimeout   time.Duration
		shutdownDeadline time.Time
		shutdownErr      error
		shutdownStarted  chan struct{}
		shutdownDone     chan struct{}
		deadlineChanged  chan struct{}
		deadlineExpired  chan struct{}
		forceStop        sync.Once
	}

	servingState uint8
)

const (
	servingIdle servingState = iota
	servingStarting
	servingActive
	servingStopped
)

// New adapts a caller-configured runtime without taking ownership
// or creating a listener. Limits default to DefaultLimits. Option failures
// are joined after all non-nil options run, before server construction.
func New(runtime api.Runtime, options ...Option) (*Server, error) {
	if isNilRuntime(runtime) {
		return nil, errors.New("runtime is required")
	}

	configured := config{limits: DefaultLimits()}
	var failures []error
	for _, option := range options {
		if option == nil {
			failures = append(failures, errors.New("server option must not be nil"))

			continue
		}

		if err := option(&configured); err != nil {
			failures = append(failures, err)
		}
	}

	if err := errors.Join(failures...); err != nil {
		return nil, err
	}

	info := grpcserver.Handshake{
		ProtocolName:      protocolName,
		ProtocolVersion:   protocolVersion,
		RuntimeName:       configured.runtimeIdentity.Name,
		RuntimeVersion:    configured.runtimeIdentity.Version,
		RuntimeInstanceID: configured.runtimeIdentity.InstanceID,
	}
	connections := core.NewConnectionRegistry(configured.limits.MaxConnections, core.ResourceLimits{
		Plans:         configured.limits.MaxPlansPerConnection,
		Sessions:      configured.limits.MaxSessionsPerConnection,
		Executions:    configured.limits.MaxExecutionsPerConnection,
		DebugSessions: configured.limits.MaxDebugSessionsPerConnection,
		Watchers:      configured.limits.MaxWatchersPerResource,
		Breakpoints:   configured.limits.MaxBreakpointsPerDebugSession,
	})

	grpcOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(configured.limits.MaxInboundMessageBytes),
		grpc.MaxSendMsgSize(configured.limits.MaxOutboundMessageBytes),
		grpc.UnaryInterceptor(grpcserver.UnaryRecoveryInterceptor),
		grpc.StreamInterceptor(grpcserver.StreamRecoveryInterceptor),
		grpc.ChainUnaryInterceptor(configured.unary...),
		grpc.ChainStreamInterceptor(configured.stream...),
	}
	if configured.credentials != nil {
		grpcOptions = append(grpcOptions, grpc.Creds(configured.credentials))
	}

	grpcServer := grpc.NewServer(grpcOptions...)
	grpcserver.New(runtime, info, connections).Register(grpcServer)

	return &Server{
		grpcServer: grpcServer, connections: connections,
		listen:          new(net.ListenConfig).Listen,
		shutdownStarted: make(chan struct{}), shutdownDone: make(chan struct{}),
		deadlineChanged: make(chan struct{}, 1), deadlineExpired: make(chan struct{}),
	}, nil
}

// Run opens a TCP listener at address and manages serving and shutdown. No default
// endpoint is supplied. Cancellation after startup returns nil if cleanup succeeds.
// Shutdown defaults to a 30-second budget, independent of ctx. A timeout matches
// context.DeadlineExceeded and leaves committed hosted cleanup observable through
// Shutdown. Option failures are joined before startup reservation or listening.
// Run never closes the borrowed runtime.
func (s *Server) Run(ctx context.Context, address string, options ...RunOption) error {
	configured := runConfig{shutdownTimeout: 30 * time.Second}
	var failures []error
	for _, option := range options {
		if option == nil {
			failures = append(failures, errors.New("run option must not be nil"))

			continue
		}

		if err := option(&configured); err != nil {
			failures = append(failures, err)
		}
	}

	if err := errors.Join(failures...); err != nil {
		return err
	}

	if address == "" {
		return errors.New("address is required")
	}

	startup, cancel, err := s.reserveStart(ctx, configured.shutdownTimeout)
	if err != nil {
		return err
	}

	defer cancel()

	listener, err := s.listen(startup, "tcp", address)
	if err != nil {
		s.abortStart()

		return err
	}

	observed := &servingListener{Listener: listener}
	if err := s.commitStart(ctx, observed); err != nil {
		return errors.Join(err, observed.Close())
	}

	serveResult := make(chan error, 1)
	go func() { serveResult <- s.grpcServer.Serve(observed) }()

	var serveErr error
	select {
	case serveErr = <-serveResult:
		serveResult = nil
	case <-ctx.Done():
	case <-s.shutdownStarted:
	}

	s.beginShutdown(time.Time{})
	done := s.shutdownDone
	for serveResult != nil || done != nil {
		select {
		case serveErr = <-serveResult:
			serveResult = nil
		case <-done:
			done = nil
		case <-s.deadlineExpired:
			select {
			case serveErr = <-serveResult:
				serveResult = nil
			default:
			}

			// Prefer completed settlement over a simultaneously expired budget.
			select {
			case <-s.shutdownDone:
				if serveResult == nil {
					return s.servingResult(observed, serveErr)
				}
			default:
			}

			return errors.Join(s.servingResult(observed, serveErr), context.DeadlineExceeded)
		}
	}

	return s.servingResult(observed, serveErr)
}

// Serve accepts a caller-created listener, including Unix sockets and custom
// transports. Once accepted, gRPC closes it when serving returns. Cancellation
// initiates shutdown; callers use Shutdown to wait for retained cleanup results.
// Serve has no default shutdown timeout. Rejected starts leave the listener untouched.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("listener is required")
	}

	_, cancel, err := s.reserveStart(ctx, 0)
	if err != nil {
		return err
	}

	defer cancel()

	if err := s.commitStart(ctx, nil); err != nil {
		return err
	}

	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.beginShutdown(deadlineFrom(ctx))
		case <-watchDone:
		}
	}()

	err = s.grpcServer.Serve(listener)
	close(watchDone)

	return normalizeServeError(err)
}

func (s *Server) reserveStart(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, errors.New("context is required")
	}

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	s.serveMu.Lock()
	defer s.serveMu.Unlock()

	if s.state == servingStopped {
		return nil, nil, grpc.ErrServerStopped
	}

	if s.state != servingIdle {
		return nil, nil, errors.New("Wire server is already serving")
	}

	startup, cancel := context.WithCancel(ctx)
	s.state = servingStarting
	s.managedTimeout = timeout
	s.startupCancel = cancel

	return startup, cancel, nil
}

func (s *Server) abortStart() {
	s.serveMu.Lock()
	defer s.serveMu.Unlock()

	if s.state == servingStarting {
		s.state = servingIdle
		s.managedTimeout = 0
		s.startupCancel = nil
	}
}

func (s *Server) commitStart(ctx context.Context, listener *servingListener) error {
	s.serveMu.Lock()
	defer s.serveMu.Unlock()

	if s.state == servingStopped {
		return grpc.ErrServerStopped
	}

	if err := ctx.Err(); err != nil {
		s.state = servingIdle
		s.managedTimeout = 0
		s.startupCancel = nil

		return err
	}

	s.state = servingActive
	s.startupCancel = nil
	s.listener = listener

	return nil
}

// Shutdown commits logical cleanup once and waits with ctx. The earliest supplied
// deadline forces transport shutdown; later calls cannot extend it or an active
// Run's budget. Transport stopping does not finish hosted cleanup. Concurrent and
// subsequent callers observe the same retained settlement result.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is required")
	}

	s.beginShutdown(deadlineFrom(ctx))
	select {
	case <-s.shutdownDone:
		return s.shutdownResult()
	case <-ctx.Done():
		select {
		case <-s.shutdownDone:
			return s.shutdownResult()
		default:
			return ctx.Err()
		}
	}
}

func (s *Server) beginShutdown(deadline time.Time) {
	s.serveMu.Lock()

	first := s.state != servingStopped
	if first {
		s.state = servingStopped
		if s.managedTimeout > 0 {
			s.shutdownDeadline = time.Now().Add(s.managedTimeout)
		}

		close(s.shutdownStarted)
	}

	if !deadline.IsZero() && (s.shutdownDeadline.IsZero() || deadline.Before(s.shutdownDeadline)) {
		s.shutdownDeadline = deadline
		select {
		case s.deadlineChanged <- struct{}{}:
		default:
		}
	}

	cancel := s.startupCancel
	s.serveMu.Unlock()

	if first {
		go s.watchShutdownDeadline()
		go s.settleShutdown()
	}

	if cancel != nil {
		cancel()
	}
}

func (s *Server) watchShutdownDeadline() {
	for {
		s.serveMu.Lock()
		deadline := s.shutdownDeadline
		s.serveMu.Unlock()

		var timer *time.Timer
		var expired <-chan time.Time

		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			expired = timer.C
		}

		select {
		case <-s.shutdownDone:
			if timer != nil {
				timer.Stop()
			}

			return
		case <-s.deadlineChanged:
			if timer != nil {
				timer.Stop()
			}
		case <-expired:
			s.stopTransport()
			close(s.deadlineExpired)

			return
		}
	}
}

func (s *Server) stopTransport() {
	// Stop can itself wait for transport handshakes. It must not own the
	// managed wait or be blocked behind uncooperative hosted cleanup.
	s.forceStop.Do(func() { go s.grpcServer.Stop() })
}

func (s *Server) settleShutdown() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, errors.New("Wire server shutdown panicked"))
			s.stopTransport()
		}

		s.serveMu.Lock()
		s.shutdownErr = err
		s.serveMu.Unlock()
		close(s.shutdownDone)
	}()

	s.serveMu.Lock()
	listener := s.listener
	s.serveMu.Unlock()

	if listener != nil {
		err = listener.Close()
		s.serveMu.Lock()
		s.shutdownErr = err
		s.serveMu.Unlock()
	}

	err = errors.Join(err, s.connections.Close(context.Background()))
	s.serveMu.Lock()
	s.shutdownErr = err
	s.serveMu.Unlock()
	s.grpcServer.GracefulStop()
}

func (s *Server) shutdownResult() error {
	s.serveMu.Lock()
	defer s.serveMu.Unlock()

	return s.shutdownErr
}

func (s *Server) servingResult(listener *servingListener, serveErr error) error {
	return errors.Join(listener.result(normalizeServeError(serveErr)), s.shutdownResult())
}
