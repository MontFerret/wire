package core

import (
	"context"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type (
	testHost struct {
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

	testEnvironment struct {
		*Connection
		host          *testHost
		plans         testPlanRegistry
		executions    testExecutionRegistry
		debugSessions testDebugSessionRegistry
	}

	testPlanRegistry struct {
		registry *PlanRegistry
		owner    ConnectionID
	}

	testExecutionRegistry struct {
		registry *ExecutionRegistry
		owner    ConnectionID
	}

	testDebugSessionRegistry struct {
		registry *DebugSessionRegistry
		owner    ConnectionID
	}

	fixtureLimits struct {
		MaxConnections                int
		MaxPlansPerConnection         int
		MaxSessionsPerConnection      int
		MaxExecutionsPerConnection    int
		MaxDebugSessionsPerConnection int
		MaxWatchersPerResource        int
		MaxBreakpointsPerDebugSession int
	}
)

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

func (r testPlanRegistry) lookup(id PlanID) (*Plan, error) {
	return r.registry.get(r.owner, id)
}

func (r testExecutionRegistry) lookup(id ExecutionID) (*Execution, error) {
	return r.registry.get(r.owner, id)
}

func (r testDebugSessionRegistry) lookup(id DebugSessionID) (*DebugSession, error) {
	return r.registry.get(r.owner, id)
}

func (e *testEnvironment) operation(ctx context.Context) (*Context, context.CancelFunc) {
	return NewContext(ctx, e.Connection)
}

func (e *testEnvironment) Compile(ctx context.Context, input CompileInput) (PlanSnapshot, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.compiler.Compile(operation, input)
}

func (e *testEnvironment) Execute(ctx context.Context, input ExecuteInput) (ExecutionRecord, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.executor.Execute(operation, input)
}

func (e *testEnvironment) CreateSession(ctx context.Context, input CreateSessionInput) (SessionSnapshot, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.executor.CreateSession(operation, input)
}

func (e *testEnvironment) RunSession(ctx context.Context, id SessionID) (ExecutionRecord, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.executor.RunSession(operation, id)
}

func (e *testEnvironment) RunRuntime(ctx context.Context, input RunRuntimeInput) (ExecutionRecord, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.executor.RunRuntime(operation, input)
}

func (e *testEnvironment) ReleaseSession(ctx context.Context, id SessionID) error {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.lifecycle.ReleaseSession(operation, id)
}

func (e *testEnvironment) CancelExecution(id ExecutionID) (ExecutionRecord, error) {
	operation, cancel := e.operation(context.Background())
	defer cancel()

	execution, err := e.host.executor.Execution(operation, id)
	if err != nil {
		return ExecutionRecord{}, err
	}

	return execution.Cancel(), nil
}

func (e *testEnvironment) WatchExecution(id ExecutionID) (ExecutionSubscription, error) {
	operation, cancel := e.operation(context.Background())
	defer cancel()

	execution, err := e.host.executor.Execution(operation, id)
	if err != nil {
		return ExecutionSubscription{}, err
	}

	return execution.Watch()
}

func (e *testEnvironment) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.lifecycle.ReleaseExecution(operation, id)
}

func (e *testEnvironment) OpenDebugSession(ctx context.Context, input OpenDebugInput) (DebugSessionRecord, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.debugger.Create(operation, input)
}

func (e *testEnvironment) debugSession(ctx context.Context, id DebugSessionID) (*Context, context.CancelFunc, *DebugSession, error) {
	operation, cancel := e.operation(ctx)
	session, err := e.host.debugger.Session(operation, id)
	if err != nil {
		cancel()

		return nil, nil, nil, err
	}

	return operation, cancel, session, nil
}

func (e *testEnvironment) WatchDebug(id DebugSessionID) (DebugSubscription, error) {
	_, cancel, session, err := e.debugSession(context.Background(), id)
	if err != nil {
		return DebugSubscription{}, err
	}
	defer cancel()

	return session.Watch()
}

func (e *testEnvironment) StartDebug(ctx context.Context, id DebugSessionID) (DebugSessionRecord, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return DebugSessionRecord{}, err
	}
	defer cancel()

	return session.Start(operation)
}

func (e *testEnvironment) ContinueDebug(ctx context.Context, id DebugSessionID) (DebugSessionRecord, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return DebugSessionRecord{}, err
	}
	defer cancel()

	return session.Continue(operation)
}

func (e *testEnvironment) StopDebug(ctx context.Context, id DebugSessionID) (DebugSessionRecord, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return DebugSessionRecord{}, err
	}
	defer cancel()

	return session.Stop(operation)
}

func (e *testEnvironment) SetBreakpoint(
	ctx context.Context,
	id DebugSessionID,
	location source.Location,
) (debugger.Breakpoint, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return debugger.Breakpoint{}, err
	}
	defer cancel()

	return session.SetBreakpoint(operation, location)
}

func (e *testEnvironment) Frames(ctx context.Context, id DebugSessionID) ([]debugger.Frame, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()

	return session.Frames(operation)
}

func (e *testEnvironment) FrameLocals(
	ctx context.Context,
	id DebugSessionID,
	frame int,
) ([]debugger.Variable, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()

	return session.FrameLocals(operation, frame)
}

func (e *testEnvironment) Variables(
	ctx context.Context,
	id DebugSessionID,
	reference debugger.ValueReference,
) ([]debugger.Variable, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return nil, err
	}
	defer cancel()

	return session.Variables(operation, reference)
}

func (e *testEnvironment) EvaluateFrame(
	ctx context.Context,
	id DebugSessionID,
	frame int,
	expression string,
) (debugger.Value, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return debugger.Value{}, err
	}
	defer cancel()

	return session.EvaluateFrame(operation, frame, expression)
}

func (e *testEnvironment) ReleaseDebugSession(ctx context.Context, id DebugSessionID) error {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.lifecycle.ReleaseDebugSession(operation, id)
}

func (e *testEnvironment) ReleasePlan(ctx context.Context, id PlanID) error {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.host.lifecycle.ReleasePlan(operation, id)
}

func (e *testEnvironment) Close(ctx context.Context) error {
	err := e.host.lifecycle.CloseConnection(ctx, e.ID())
	if hasCategory(err, ErrorKindConnectionNotFound) && e.close.Started() {
		return e.waitClose(ctx)
	}

	return err
}
