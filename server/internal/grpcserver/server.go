package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
	"google.golang.org/grpc"
)

// Server composes the protocol services over shared core resource owners.
type Server struct {
	runtime    *RuntimeService
	plans      *PlanService
	sessions   *SessionService
	executions *ExecutionService
	debug      *DebugService
}

func New(
	info core.RuntimeInfo,
	connections *core.ConnectionRegistry,
	compiler *core.Compiler,
	executor *core.Executor,
	debugger *core.Debugger,
	lifecycle *core.Lifecycle,
) *Server {
	operations := &operationContextFactory{connections: connections}

	return &Server{
		runtime:    &RuntimeService{info: info, connections: connections, executor: executor, lifecycle: lifecycle, operations: operations},
		plans:      &PlanService{compiler: compiler, lifecycle: lifecycle, operations: operations},
		sessions:   &SessionService{executor: executor, lifecycle: lifecycle, operations: operations},
		executions: &ExecutionService{executor: executor, lifecycle: lifecycle, operations: operations},
		debug:      &DebugService{debugger: debugger, lifecycle: lifecycle, operations: operations},
	}
}

func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	wirev1.RegisterRuntimeServiceServer(registrar, s.runtime)
	wirev1.RegisterPlanServiceServer(registrar, s.plans)
	wirev1.RegisterSessionServiceServer(registrar, s.sessions)
	wirev1.RegisterExecutionServiceServer(registrar, s.executions)
	wirev1.RegisterDebugServiceServer(registrar, s.debug)
}
