package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
	"google.golang.org/grpc"
)

type Server struct {
	wirev1.UnimplementedRuntimeServiceServer
	wirev1.UnimplementedPlanServiceServer
	wirev1.UnimplementedSessionServiceServer
	wirev1.UnimplementedExecutionServiceServer
	wirev1.UnimplementedDebugServiceServer

	info        core.RuntimeInfo
	connections *core.ConnectionRegistry
	compiler    *core.Compiler
	executor    *core.Executor
	debugger    *core.Debugger
	lifecycle   *core.Lifecycle
}

func New(
	info core.RuntimeInfo,
	connections *core.ConnectionRegistry,
	compiler *core.Compiler,
	executor *core.Executor,
	debugger *core.Debugger,
	lifecycle *core.Lifecycle,
) *Server {
	return &Server{
		info:        info,
		connections: connections,
		compiler:    compiler,
		executor:    executor,
		debugger:    debugger,
		lifecycle:   lifecycle,
	}
}

func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	wirev1.RegisterRuntimeServiceServer(registrar, s)
	wirev1.RegisterPlanServiceServer(registrar, s)
	wirev1.RegisterSessionServiceServer(registrar, s)
	wirev1.RegisterExecutionServiceServer(registrar, s)
	wirev1.RegisterDebugServiceServer(registrar, s)
}

func (s *Server) operationContext(
	parent context.Context,
	id *wirev1.ConnectionId,
) (*core.Context, context.CancelFunc, error) {
	connection, err := s.connections.Get(core.ConnectionID(id.GetValue()))
	if err != nil {
		return nil, nil, rpcError(err)
	}

	ctx, cancel := core.NewContext(parent, connection)

	return ctx, cancel, nil
}
