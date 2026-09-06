package core

import (
	"context"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
)

type testEnvironment struct {
	*Connection
	host *testHost
}

func (e *testEnvironment) operation(ctx context.Context) (context.Context, context.CancelFunc) {
	return OperationContext(ctx, e.Context())
}

func (e *testEnvironment) Compile(ctx context.Context, input compileRequest) (planResult, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	var options []api.PlanOption

	if input.HasOptimizationLevel {
		options = append(options, api.WithOptimizationLevel(input.OptimizationLevel))
	}

	plan, err := CompilePlan(operation, e.host.runtime, e.resources, input.Source, input.Debuggable, options...)
	if err != nil {
		return planResult{}, err
	}

	return planResult{ID: plan.ID(), Parameters: plan.Params()}, nil
}

func (e *testEnvironment) Execute(ctx context.Context, input executeRequest) (executionResult, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	if err := e.resources.operationError(operation); err != nil {
		return executionResult{}, err
	}

	plan, err := e.resources.Plan(operation, input.PlanID)
	if err != nil {
		return executionResult{}, err
	}

	execution, err := plan.Execute(operation, apiSessionOptions(input.Parameters, input.OutputContentType)...)
	if err != nil {
		return executionResult{}, err
	}

	return executionResult{ID: execution.ID(), Snapshot: execution.Snapshot()}, nil
}

func (e *testEnvironment) CreateSession(ctx context.Context, input sessionRequest) (SessionID, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	if err := e.resources.operationError(operation); err != nil {
		return "", err
	}

	plan, err := e.resources.Plan(operation, input.PlanID)
	if err != nil {
		return "", err
	}

	session, err := plan.NewSession(operation, apiSessionOptions(input.Parameters, input.OutputContentType)...)
	if err != nil {
		return "", err
	}

	return session.ID(), nil
}

func (e *testEnvironment) RunSession(ctx context.Context, id SessionID) (executionResult, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	if err := e.resources.operationError(operation); err != nil {
		return executionResult{}, err
	}

	session, err := e.resources.Session(operation, id)
	if err != nil {
		return executionResult{}, err
	}

	execution, err := session.Execute(operation)
	if err != nil {
		return executionResult{}, err
	}

	return executionResult{ID: execution.ID(), Snapshot: wireexecution.Snapshot{State: wireexecution.StateRunning}}, nil
}

func (e *testEnvironment) Run(ctx context.Context, input runRequest) (executionResult, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	execution, err := Run(operation, e.host.runtime, e.resources, input.Source, apiSessionOptions(input.Parameters, input.OutputContentType)...)
	if err != nil {
		return executionResult{}, err
	}

	return executionResult{ID: execution.ID(), Snapshot: wireexecution.Snapshot{State: wireexecution.StateRunning}}, nil
}

func (e *testEnvironment) ReleaseSession(ctx context.Context, id SessionID) error {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.resources.ReleaseSession(operation, id)
}

func (e *testEnvironment) CancelExecution(id ExecutionID) (executionResult, error) {
	operation, cancel := e.operation(context.Background())
	defer cancel()

	execution, err := e.resources.Execution(operation, id)
	if err != nil {
		return executionResult{}, err
	}

	return executionResult{ID: execution.ID(), Snapshot: execution.Cancel()}, nil
}

func (e *testEnvironment) WatchExecution(id ExecutionID) (ExecutionSubscription, error) {
	operation, cancel := e.operation(context.Background())
	defer cancel()

	execution, err := e.resources.Execution(operation, id)
	if err != nil {
		return ExecutionSubscription{}, err
	}

	return execution.Watch()
}

func (e *testEnvironment) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.resources.ReleaseExecution(operation, id)
}

func (e *testEnvironment) OpenDebugSession(ctx context.Context, input debugRequest) (debugResult, error) {
	operation, cancel := e.operation(ctx)
	defer cancel()

	if err := e.resources.operationError(operation); err != nil {
		return debugResult{}, err
	}

	plan, err := e.resources.Plan(operation, input.PlanID)
	if err != nil {
		return debugResult{}, err
	}

	session, err := plan.NewDebugSession(operation, apiSessionOptions(input.Parameters, input.OutputContentType)...)
	if err != nil {
		return debugResult{}, err
	}

	return debugResult{ID: session.ID(), Snapshot: session.Snapshot()}, nil
}

func (e *testEnvironment) debugSession(ctx context.Context, id DebugSessionID) (context.Context, context.CancelFunc, *DebugSession, error) {
	operation, cancel := e.operation(ctx)

	session, err := e.resources.DebugSession(operation, id)
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

func (e *testEnvironment) StartDebug(ctx context.Context, id DebugSessionID) (debugResult, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return debugResult{}, err
	}

	defer cancel()

	snapshot, err := session.Start(operation)

	return debugResult{ID: session.ID(), Snapshot: snapshot}, err
}

func (e *testEnvironment) ContinueDebug(ctx context.Context, id DebugSessionID) (debugResult, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return debugResult{}, err
	}

	defer cancel()

	snapshot, err := session.Continue(operation)

	return debugResult{ID: session.ID(), Snapshot: snapshot}, err
}

func (e *testEnvironment) StopDebug(ctx context.Context, id DebugSessionID) (debugResult, error) {
	operation, cancel, session, err := e.debugSession(ctx, id)
	if err != nil {
		return debugResult{}, err
	}

	defer cancel()

	snapshot, err := session.Stop(operation)

	return debugResult{ID: session.ID(), Snapshot: snapshot}, err
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

	return e.resources.ReleaseDebugSession(operation, id)
}

func (e *testEnvironment) ReleasePlan(ctx context.Context, id PlanID) error {
	operation, cancel := e.operation(ctx)
	defer cancel()

	return e.resources.ReleasePlan(operation, id)
}

func (e *testEnvironment) Close(ctx context.Context) error {
	err := e.host.connections.CloseConnection(ctx, e.ID())
	if hasCategory(err, ErrorKindConnectionNotFound) && e.close.Started() {
		return e.waitClose(ctx)
	}

	return err
}
