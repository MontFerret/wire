package wire

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/MontFerret/ferret/v2"
	"github.com/MontFerret/ferret/v2/pkg/compiler"
	"github.com/MontFerret/wire/internal/core"
	"github.com/MontFerret/wire/internal/grpcserver"
	"github.com/MontFerret/wire/internal/lifecycle"
	"google.golang.org/grpc"
)

// Server hosts Ferret Wire over a caller-supplied listener. It borrows the
// Engine passed to NewServer and never closes it.
type Server struct {
	grpcServer *grpc.Server
	runtime    *core.Runtime

	serveMu  sync.Mutex
	serving  bool
	shutdown lifecycle.Close
}

// NewServer adapts a caller-configured Ferret engine without taking ownership
// or creating a listener. Limits default to DefaultServerLimits.
func NewServer(engine *ferret.Engine, options ...ServerOption) (*Server, error) {
	configured := serverOptions{limits: DefaultServerLimits()}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("server option must not be nil")
		}

		if err := option.apply(&configured); err != nil {
			return nil, err
		}
	}

	info := core.RuntimeInfo{
		APIIdentity:   apiIdentity,
		WireVersion:   moduleVersion(wireModulePath, "devel"),
		FerretVersion: moduleVersion(ferretModulePath, compiler.Version),
		RuntimeIdentity: core.RuntimeIdentity{
			Name:       configured.runtimeIdentity.Name,
			Version:    configured.runtimeIdentity.Version,
			InstanceID: configured.runtimeIdentity.InstanceID,
		},
	}
	runtime, err := core.NewRuntime(engine, info, core.Limits{
		MaxConnections:                configured.limits.MaxConnections,
		MaxPlansPerConnection:         configured.limits.MaxPlansPerConnection,
		MaxExecutionsPerConnection:    configured.limits.MaxExecutionsPerConnection,
		MaxDebugSessionsPerConnection: configured.limits.MaxDebugSessionsPerConnection,
		MaxWatchersPerResource:        configured.limits.MaxWatchersPerResource,
		MaxBreakpointsPerDebugSession: configured.limits.MaxBreakpointsPerDebugSession,
	})
	if err != nil {
		return nil, err
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(configured.limits.MaxInboundMessageBytes),
		grpc.MaxSendMsgSize(configured.limits.MaxOutboundMessageBytes),
		grpc.UnaryInterceptor(grpcserver.UnaryRecoveryInterceptor),
		grpc.StreamInterceptor(grpcserver.StreamRecoveryInterceptor),
	)
	grpcserver.New(runtime).Register(grpcServer)

	return &Server{grpcServer: grpcServer, runtime: runtime}, nil
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

	err = s.runtime.Close(context.Background())
	s.grpcServer.GracefulStop()
}

func deadlineFrom(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}

	return deadline
}
