package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
	"google.golang.org/grpc"
)

type Server struct {
	wirev1.UnimplementedRuntimeServiceServer
	wirev1.UnimplementedPlanServiceServer
	wirev1.UnimplementedExecutionServiceServer
	wirev1.UnimplementedDebugServiceServer

	runtime *core.Runtime
}

func New(runtime *core.Runtime) *Server {
	return &Server{runtime: runtime}
}

func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	wirev1.RegisterRuntimeServiceServer(registrar, s)
	wirev1.RegisterPlanServiceServer(registrar, s)
	wirev1.RegisterExecutionServiceServer(registrar, s)
	wirev1.RegisterDebugServiceServer(registrar, s)
}

func (s *Server) connection(id *wirev1.ConnectionId) (*core.Connection, error) {
	connection, err := s.runtime.Connection(core.ConnectionID(id.GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	return connection, nil
}
