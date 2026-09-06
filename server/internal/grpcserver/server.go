// Package grpcserver translates Wire RPCs into core operations and sanitized protocol responses.
package grpcserver

import (
	"google.golang.org/grpc"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// Server composes the protocol services over shared core resource owners.
type Server struct {
	runtime    *RuntimeService
	plans      *PlanService
	sessions   *SessionService
	executions *ExecutionService
	debug      *DebugService
}

// New composes RPC services over the caller's runtime and logical connection registry.
func New(runtime api.Runtime, info Handshake, connections *core.ConnectionRegistry) *Server {
	return &Server{
		runtime:    &RuntimeService{runtime: runtime, info: info, connections: connections},
		plans:      &PlanService{runtime: runtime, connections: connections},
		sessions:   &SessionService{connections: connections},
		executions: &ExecutionService{connections: connections},
		debug:      &DebugService{connections: connections},
	}
}

// Register installs all five Wire services on the supplied gRPC registrar.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	wirev1.RegisterRuntimeServiceServer(registrar, s.runtime)
	wirev1.RegisterPlanServiceServer(registrar, s.plans)
	wirev1.RegisterSessionServiceServer(registrar, s.sessions)
	wirev1.RegisterExecutionServiceServer(registrar, s.executions)
	wirev1.RegisterDebugServiceServer(registrar, s.debug)
}
