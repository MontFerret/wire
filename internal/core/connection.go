package core

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

// Connection is the logical ownership scope established by RuntimeService.Connect.
type Connection struct {
	mu                sync.RWMutex
	id                ConnectionID
	runtime           api.Runtime
	ctx               context.Context
	cancel            context.CancelCauseFunc
	plans             map[PlanID]*Plan
	executions        map[ExecutionID]*Execution
	debug             map[DebugSessionID]*DebugSession
	closingPlans      map[PlanID]*Plan
	closingExecutions map[ExecutionID]*Execution
	closingDebug      map[DebugSessionID]*DebugSession
	limits            Limits
	pendingPlans      int
	pendingDebug      int
	operations        sync.WaitGroup
	closed            bool
	close             lifecycle.Close
}

func newConnection(id ConnectionID, runtime api.Runtime, limits Limits) *Connection {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &Connection{
		id:                id,
		runtime:           runtime,
		ctx:               ctx,
		cancel:            cancel,
		plans:             make(map[PlanID]*Plan),
		executions:        make(map[ExecutionID]*Execution),
		debug:             make(map[DebugSessionID]*DebugSession),
		closingPlans:      make(map[PlanID]*Plan),
		closingExecutions: make(map[ExecutionID]*Execution),
		closingDebug:      make(map[DebugSessionID]*DebugSession),
		limits:            limits,
	}
}

func (c *Connection) ID() ConnectionID {
	return c.id
}

func (c *Connection) Context() context.Context {
	return c.ctx
}

func (c *Connection) ReleaseDebugSession(ctx context.Context, id DebugSessionID) error {
	if err := validateID(id, "debug session ID"); err != nil {
		return err
	}
	c.mu.Lock()
	session := c.debug[id]
	if session != nil {
		delete(c.debug, id)
		c.closingDebug[id] = session
	} else {
		session = c.closingDebug[id]
	}

	if session != nil {
		session.plan.mu.Lock()
		delete(session.plan.debug, id)
		session.plan.mu.Unlock()
	}
	c.mu.Unlock()
	if session == nil {
		return notFound(ErrorDebugSessionNotFound, string(id))
	}

	if session.release.Begin() {
		go c.settleDebugRelease(session)
	}

	return session.release.Wait(ctx)
}

func (c *Connection) StopDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}
	snapshot := session.snapshot()
	if !snapshot.State.terminal() {
		if err := session.Close(ctx); err != nil {
			return DebugSnapshot{}, err
		}
		snapshot = session.snapshot()
	}

	return snapshot, nil
}

func (c *Connection) WatchDebug(id DebugSessionID) (DebugSubscription, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSubscription{}, err
	}

	return session.subscribe()
}

func (c *Connection) PauseDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if session.state != DebugRunning {
		return DebugSnapshot{}, invalidState("debug session is not running", nil)
	}

	if err := session.debugger.Pause(); err != nil {
		return DebugSnapshot{}, invalidState("pause failed", err)
	}

	return session.snapshotLocked(), nil
}

func (c *Connection) SetBreakpoint(ctx context.Context, id DebugSessionID, location source.Location) (debugger.Breakpoint, error) {
	if err := ctx.Err(); err != nil {
		return debugger.Breakpoint{}, err
	}

	if location.File == "" {
		return debugger.Breakpoint{}, invalidRequest("breakpoint file is required")
	}

	if location.Line <= 0 {
		return debugger.Breakpoint{}, invalidRequest("breakpoint line must be positive")
	}

	if location.Column < 0 {
		return debugger.Breakpoint{}, invalidRequest("breakpoint column must not be negative")
	}

	session, err := c.debugSession(id)
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if session.state != DebugCreated && session.state != DebugStopped {
		return debugger.Breakpoint{}, invalidState("breakpoints require a created or stopped debug session", nil)
	}

	if len(session.breakpoints) >= session.maxBreakpoints {
		return debugger.Breakpoint{}, resourceExhausted("breakpoint limit reached")
	}

	if err := ctx.Err(); err != nil {
		return debugger.Breakpoint{}, err
	}

	value, err := session.debugger.SetBreakpointAt(
		location,
		debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindNextExecutableInFile},
	)
	if err != nil {
		return debugger.Breakpoint{}, invalidState("set breakpoint failed", err)
	}

	session.breakpoints[value.ID] = value

	return value, nil
}

func (c *Connection) DeleteBreakpoint(ctx context.Context, id DebugSessionID, breakpointID debugger.BreakpointID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if breakpointID <= 0 {
		return invalidRequest("breakpoint ID must be positive")
	}

	session, err := c.debugSession(id)
	if err != nil {
		return err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if session.state != DebugCreated && session.state != DebugStopped {
		return invalidState("breakpoints require a created or stopped debug session", nil)
	}

	value, exists := session.breakpoints[breakpointID]
	if !exists {
		return notFound(ErrorBreakpointNotFound, fmt.Sprint(breakpointID))
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := session.debugger.DeleteBreakpoint(value.ID); err != nil {
		return invalidState("delete breakpoint failed", err)
	}

	delete(session.breakpoints, breakpointID)

	return nil
}

func (c *Connection) Frames(ctx context.Context, id DebugSessionID) ([]debugger.Frame, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return nil, err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireStoppedLocked(ctx); err != nil {
		return nil, err
	}

	values, err := session.debugger.Frames()
	if err != nil {
		return nil, invalidState("frames failed", err)
	}

	return append([]debugger.Frame(nil), values...), nil
}

func (c *Connection) FrameLocals(ctx context.Context, id DebugSessionID, frame int) ([]debugger.Variable, error) {
	if frame < 0 {
		return nil, invalidRequest("frame index must not be negative")
	}

	session, err := c.debugSession(id)
	if err != nil {
		return nil, err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireStoppedLocked(ctx); err != nil {
		return nil, err
	}

	values, err := session.debugger.FrameLocals(frame)
	if err != nil {
		return nil, invalidState("frame locals failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (c *Connection) Variables(ctx context.Context, id DebugSessionID, reference debugger.ValueReference) ([]debugger.Variable, error) {
	if reference <= 0 {
		return nil, invalidRequest("value reference must be positive")
	}

	session, err := c.debugSession(id)
	if err != nil {
		return nil, err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireStoppedLocked(ctx); err != nil {
		return nil, err
	}

	values, err := session.debugger.Variables(reference)
	if err != nil {
		return nil, invalidState("variables failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (c *Connection) EvaluateFrame(ctx context.Context, id DebugSessionID, frame int, expression string) (debugger.Value, error) {
	if frame < 0 {
		return debugger.Value{}, invalidRequest("frame index must not be negative")
	}

	if expression == "" {
		return debugger.Value{}, invalidRequest("expression is required")
	}

	session, err := c.debugSession(id)
	if err != nil {
		return debugger.Value{}, err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.requireStoppedLocked(ctx); err != nil {
		return debugger.Value{}, err
	}

	evaluateCtx, cancel := session.operationContext(ctx)
	defer cancel()

	value, err := session.debugger.EvaluateFrame(evaluateCtx, frame, expression)
	if err != nil {
		return debugger.Value{}, invalidState("evaluation failed", err)
	}

	return value, nil
}

func (c *Connection) OpenDebugSession(ctx context.Context, input OpenDebugInput) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return DebugSnapshot{}, err
	}

	if err := c.beginDebugCreation(); err != nil {
		return DebugSnapshot{}, err
	}
	defer c.finishDebugCreation()

	c.mu.Lock()
	if err := c.ensureOpenLocked(); err != nil {
		c.mu.Unlock()
		return DebugSnapshot{}, err
	}

	plan := c.plans[input.PlanID]
	if plan == nil {
		c.mu.Unlock()
		return DebugSnapshot{}, notFound(ErrorPlanNotFound, string(input.PlanID))
	}

	plan.mu.Lock()
	if plan.closing {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, notFound(ErrorPlanNotFound, string(input.PlanID))
	}

	if !plan.debuggable {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, invalidState("plan was not compiled for debugging", nil)
	}
	plan.mu.Unlock()
	c.mu.Unlock()

	options := []api.SessionOption{api.WithParams(cloneParameters(input.Parameters))}
	if input.OutputContentType != "" {
		options = append(options, api.WithOutputContentType(input.OutputContentType))
	}

	openCtx, cancelOpen := c.operationContext(ctx)
	defer cancelOpen()

	runtimeDebugger, err := plan.plan.NewDebugSession(openCtx, options...)
	if err != nil {
		if !isNil(runtimeDebugger) {
			return DebugSnapshot{}, errors.Join(internalError(err), closeAPIDebugSession(runtimeDebugger))
		}

		return DebugSnapshot{}, internalError(err)
	}

	if isNil(runtimeDebugger) {
		return DebugSnapshot{}, internalError(errors.New("runtime returned no debug session"))
	}

	if err := openCtx.Err(); err != nil {
		return DebugSnapshot{}, errors.Join(err, closeAPIDebugSession(runtimeDebugger))
	}

	debugCtx, cancel := context.WithCancelCause(c.ctx)
	created := &DebugSession{
		id:             DebugSessionID(uuid.NewString()),
		plan:           plan,
		debugger:       runtimeDebugger,
		ctx:            debugCtx,
		cancel:         cancel,
		state:          DebugCreated,
		breakpoints:    make(map[debugger.BreakpointID]debugger.Breakpoint),
		maxWatchers:    c.limits.MaxWatchersPerResource,
		maxBreakpoints: c.limits.MaxBreakpointsPerDebugSession,
		watchers:       make(map[uint64]*debugWatcher),
	}

	c.mu.Lock()
	if err := c.ensureOpenLocked(); err != nil {
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(err, closeAPIDebugSession(runtimeDebugger))
	}

	current := c.plans[input.PlanID]
	if current != plan {
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(notFound(ErrorPlanNotFound, string(input.PlanID)), closeAPIDebugSession(runtimeDebugger))
	}

	plan.mu.Lock()
	if plan.closing {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(notFound(ErrorPlanNotFound, string(input.PlanID)), closeAPIDebugSession(runtimeDebugger))
	}

	if err := ctx.Err(); err != nil {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(err, closeAPIDebugSession(runtimeDebugger))
	}

	plan.debug[created.id] = struct{}{}
	plan.mu.Unlock()
	c.debug[created.id] = created
	c.mu.Unlock()

	return created.snapshot(), nil
}

func (c *Connection) debugSession(id DebugSessionID) (*DebugSession, error) {
	if err := validateID(id, "debug session ID"); err != nil {
		return nil, err
	}

	c.mu.RLock()
	session := c.debug[id]
	c.mu.RUnlock()

	if session == nil {
		return nil, notFound(ErrorDebugSessionNotFound, string(id))
	}

	return session, nil
}

func (c *Connection) StartDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, true, session.debugger.Start)
}

func (c *Connection) ContinueDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Continue)
}

func (c *Connection) NextDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Next)
}

func (c *Connection) StepDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Step)
}

func (c *Connection) OutDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Out)
}

func (c *Connection) execution(id ExecutionID) (*Execution, error) {
	if err := validateID(id, "execution ID"); err != nil {
		return nil, err
	}
	c.mu.RLock()
	execution := c.executions[id]
	c.mu.RUnlock()
	if execution == nil {
		return nil, notFound(ErrorExecutionNotFound, string(id))
	}

	return execution, nil
}

func (c *Connection) CancelExecution(id ExecutionID) (ExecutionSnapshot, error) {
	execution, err := c.execution(id)
	if err != nil {
		return ExecutionSnapshot{}, err
	}
	execution.cancel(context.Canceled)

	return execution.snapshot(), nil
}

func (c *Connection) WatchExecution(id ExecutionID) (ExecutionSubscription, error) {
	execution, err := c.execution(id)
	if err != nil {
		return ExecutionSubscription{}, err
	}

	return execution.subscribe()
}

func (c *Connection) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	if err := validateID(id, "execution ID"); err != nil {
		return err
	}
	c.mu.Lock()
	execution := c.executions[id]
	if execution != nil {
		delete(c.executions, id)
		c.closingExecutions[id] = execution
	} else {
		execution = c.closingExecutions[id]
	}

	if execution != nil {
		execution.plan.mu.Lock()
		delete(execution.plan.executions, id)
		execution.plan.mu.Unlock()
	}
	c.mu.Unlock()
	if execution == nil {
		return notFound(ErrorExecutionNotFound, string(id))
	}

	if execution.close.Begin() {
		go c.settleExecutionRelease(execution)
	}

	return execution.close.Wait(ctx)
}

func (c *Connection) Close(ctx context.Context) error {
	if c.close.Begin() {
		go func() {
			var err error
			defer func() {
				if recover() != nil {
					err = errors.Join(err, internalError(errors.New("connection cleanup panicked")))
				}

				c.close.Finish(err)
			}()

			err = c.settleClose()
		}()
	}

	return c.close.Wait(ctx)
}

func (c *Connection) settleExecutionRelease(execution *Execution) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("execution cleanup panicked")))
		}

		c.mu.Lock()
		if c.closingExecutions[execution.id] == execution {
			delete(c.closingExecutions, execution.id)
		}
		c.mu.Unlock()
		execution.close.Finish(err)
	}()

	execution.cancel(context.Canceled)
	<-execution.done
	execution.mu.Lock()
	for id, watcher := range execution.watchers {
		execution.closeWatcherLocked(id, watcher, nil)
	}
	execution.mu.Unlock()
}

func (c *Connection) settleDebugRelease(session *DebugSession) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("debug session release panicked")))
		}

		c.finishDebugRelease(session)
		session.release.Finish(err)
	}()

	err = session.Close(context.Background())
}

func (c *Connection) finishDebugRelease(session *DebugSession) {
	c.mu.Lock()
	if c.closingDebug[session.id] == session {
		delete(c.closingDebug, session.id)
	}
	c.mu.Unlock()
}

func (c *Connection) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	operation, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(c.ctx, func() {
		cancel(context.Cause(c.ctx))
	})

	return operation, func() {
		stop()
		cancel(context.Canceled)
	}
}

func (c *Connection) settleClose() error {
	c.mu.Lock()
	c.closed = true
	c.cancel(context.Canceled)
	c.mu.Unlock()

	c.operations.Wait()

	c.mu.Lock()
	debugIDs := make([]DebugSessionID, 0, len(c.debug)+len(c.closingDebug))
	for id := range c.debug {
		debugIDs = append(debugIDs, id)
	}

	for id := range c.closingDebug {
		if _, active := c.debug[id]; !active {
			debugIDs = append(debugIDs, id)
		}
	}
	executionIDs := make([]ExecutionID, 0, len(c.executions)+len(c.closingExecutions))
	for id := range c.executions {
		executionIDs = append(executionIDs, id)
	}

	for id := range c.closingExecutions {
		if _, active := c.executions[id]; !active {
			executionIDs = append(executionIDs, id)
		}
	}
	planIDs := make([]PlanID, 0, len(c.plans)+len(c.closingPlans))
	for id := range c.plans {
		planIDs = append(planIDs, id)
	}

	for id := range c.closingPlans {
		if _, active := c.plans[id]; !active {
			planIDs = append(planIDs, id)
		}
	}
	c.mu.Unlock()

	var result error
	for _, id := range debugIDs {
		err := c.ReleaseDebugSession(context.Background(), id)
		result = errors.Join(result, ignoreMissingResource(err, ErrorDebugSessionNotFound))
	}

	for _, id := range executionIDs {
		err := c.ReleaseExecution(context.Background(), id)
		result = errors.Join(result, ignoreMissingResource(err, ErrorExecutionNotFound))
	}

	for _, id := range planIDs {
		err := c.ReleasePlan(context.Background(), id)
		result = errors.Join(result, ignoreMissingResource(err, ErrorPlanNotFound))
	}

	c.mu.Lock()
	clear(c.debug)
	clear(c.closingDebug)
	clear(c.executions)
	clear(c.closingExecutions)
	clear(c.plans)
	clear(c.closingPlans)
	c.mu.Unlock()

	return result
}

func (c *Connection) beginPlanCreation() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureOpenLocked(); err != nil {
		return err
	}

	if c.pendingPlans+len(c.plans)+len(c.closingPlans) >= c.limits.MaxPlansPerConnection {
		return resourceExhausted("plan limit reached")
	}

	c.pendingPlans++
	c.operations.Add(1)

	return nil
}

func (c *Connection) finishPlanCreation() {
	c.mu.Lock()
	c.pendingPlans--
	c.mu.Unlock()
	c.operations.Done()
}

func (c *Connection) beginDebugCreation() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureOpenLocked(); err != nil {
		return err
	}

	if c.pendingDebug+len(c.debug)+len(c.closingDebug) >= c.limits.MaxDebugSessionsPerConnection {
		return resourceExhausted("debug session limit reached")
	}

	c.pendingDebug++
	c.operations.Add(1)

	return nil
}

func (c *Connection) finishDebugCreation() {
	c.mu.Lock()
	c.pendingDebug--
	c.mu.Unlock()
	c.operations.Done()
}

func (c *Connection) ensureOpenLocked() error {
	if c.closed {
		return invalidState("connection is closed", context.Canceled)
	}

	return nil
}
