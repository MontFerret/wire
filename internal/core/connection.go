package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/ferret/v2"
	"github.com/MontFerret/wire/internal/lifecycle"
)

// Connection is the logical ownership scope established by RuntimeService.Connect.
type Connection struct {
	mu                sync.RWMutex
	id                ConnectionID
	engine            *ferret.Engine
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

func newConnection(id ConnectionID, engine *ferret.Engine, limits Limits) *Connection {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &Connection{
		id:                id,
		engine:            engine,
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
