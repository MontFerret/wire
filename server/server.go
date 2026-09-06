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
	"github.com/MontFerret/wire/server/internal/lifecycle"
)

// Server hosts Ferret Wire over a caller-supplied listener. It borrows the
// runtime passed to NewServer and never closes it.
type Server struct {
	grpcServer  *grpc.Server
	connections *core.ConnectionRegistry

	serveMu  sync.Mutex
	serving  bool
	shutdown lifecycle.Close
}

// NewServer adapts a caller-configured runtime without taking ownership
// or creating a listener. Limits default to DefaultLimits.
func NewServer(runtime api.Runtime, options ...Option) (*Server, error) {
	if isNilRuntime(runtime) {
		return nil, errors.New("runtime is required")
	}

	configured := config{limits: DefaultLimits()}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("server option must not be nil")
		}

		if err := option.apply(&configured); err != nil {
			return nil, err
		}
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
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(configured.limits.MaxInboundMessageBytes),
		grpc.MaxSendMsgSize(configured.limits.MaxOutboundMessageBytes),
		grpc.UnaryInterceptor(grpcserver.UnaryRecoveryInterceptor),
		grpc.StreamInterceptor(grpcserver.StreamRecoveryInterceptor),
	)
	grpcserver.New(runtime, info, connections).Register(grpcServer)

	return &Server{grpcServer: grpcServer, connections: connections}, nil
}

// Serve serves the caller-owned listener until it fails, ctx is cancelled, or
// Shutdown is called. NewServer and package initialization never open a listener.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("listener is required")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	s.serveMu.Lock()
	if s.serving {
		s.serveMu.Unlock()

		return errors.New("Wire server is already serving")
	}

	s.serving = true
	s.serveMu.Unlock()

	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.beginShutdown(deadlineFrom(ctx))
		case <-watchDone:
		}
	}()

	err := s.grpcServer.Serve(listener)
	close(watchDone)

	if errors.Is(err, grpc.ErrServerStopped) || ctx.Err() != nil {
		err = nil
	}

	return err
}

// Shutdown cancels every logical connection before gracefully stopping gRPC.
// A deadline forces the gRPC server to stop, while cleanup continues once for
// all concurrent callers.
func (s *Server) Shutdown(ctx context.Context) error {
	s.beginShutdown(deadlineFrom(ctx))

	return s.shutdown.Wait(ctx)
}

func (s *Server) beginShutdown(deadline time.Time) {
	if !s.shutdown.Begin() {
		return
	}

	go s.settleShutdown(deadline)
}

func (s *Server) settleShutdown(deadline time.Time) {
	stopTimer := make(chan struct{})
	var stopOnce sync.Once
	stopDeadline := func() {
		stopOnce.Do(func() { close(stopTimer) })
	}

	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, errors.New("Wire server shutdown panicked"))
			s.grpcServer.Stop()
		}

		stopDeadline()
		s.shutdown.Finish(err)
	}()

	if !deadline.IsZero() {
		go func() {
			timer := time.NewTimer(time.Until(deadline))
			defer timer.Stop()
			select {
			case <-timer.C:
				s.grpcServer.Stop()
			case <-stopTimer:
			}
		}()
	}

	err = s.connections.Close(context.Background())
	s.grpcServer.GracefulStop()
}

func deadlineFrom(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}

	return deadline
}
