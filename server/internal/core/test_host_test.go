package core

import (
	"context"

	"github.com/MontFerret/api"
)

type testHost struct {
	connections *ConnectionRegistry
	plans       *PlanRegistry
	normal      *SessionRegistry
	executions  *ExecutionRegistry
	sessions    *DebugSessionRegistry
	compiler    *Compiler
	executor    *Executor
	debugger    *Debugger
	lifecycle   *Lifecycle
}

func newTestHost(runtime api.Runtime, limits fixtureLimits) (*testHost, error) {
	connections := NewConnectionRegistry(limits.MaxConnections)
	plans := NewPlanRegistry(limits.MaxPlansPerConnection)
	normal := NewSessionRegistry(limits.MaxSessionsPerConnection)
	executions := NewExecutionRegistry(limits.MaxExecutionsPerConnection, limits.MaxWatchersPerResource)
	sessions := NewDebugSessionRegistry(
		limits.MaxDebugSessionsPerConnection,
		limits.MaxWatchersPerResource,
		limits.MaxBreakpointsPerDebugSession,
	)
	compiler, err := NewCompiler(runtime, plans)
	if err != nil {
		return nil, err
	}

	return &testHost{
		connections: connections,
		plans:       plans,
		normal:      normal,
		executions:  executions,
		sessions:    sessions,
		compiler:    compiler,
		executor:    NewExecutor(runtime, plans, normal, executions),
		debugger:    NewDebugger(plans, sessions),
		lifecycle:   NewLifecycle(connections, plans, normal, executions, sessions),
	}, nil
}

func (h *testHost) OpenConnection() (*testEnvironment, error) {
	connection := NewConnection()
	if err := h.connections.Register(connection); err != nil {
		return nil, err
	}

	return &testEnvironment{
		Connection: connection,
		host:       h,
		plans: testPlanRegistry{
			registry: h.plans,
			owner:    connection.ID(),
		},
		executions: testExecutionRegistry{
			registry: h.executions,
			owner:    connection.ID(),
		},
		debugSessions: testDebugSessionRegistry{
			registry: h.sessions,
			owner:    connection.ID(),
		},
	}, nil
}

func (h *testHost) CloseConnection(ctx context.Context, id ConnectionID) error {
	return h.lifecycle.CloseConnection(ctx, id)
}

func (h *testHost) Close(ctx context.Context) error {
	return h.lifecycle.Close(ctx)
}
