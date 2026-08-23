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
	mu                 sync.RWMutex
	id                 ConnectionID
	engine             *ferret.Engine
	ctx                context.Context
	cancel             context.CancelCauseFunc
	plans              map[PlanID]*Plan
	executions         map[ExecutionID]*Execution
	debug              map[DebugSessionID]*DebugSession
	releasedPlans      map[PlanID]*Plan
	releasedExecutions map[ExecutionID]*Execution
	releasedDebug      map[DebugSessionID]*DebugSession
	closed             bool
	close              lifecycle.Close
}

func newConnection(id ConnectionID, engine *ferret.Engine) *Connection {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &Connection{
		id:                 id,
		engine:             engine,
		ctx:                ctx,
		cancel:             cancel,
		plans:              make(map[PlanID]*Plan),
		executions:         make(map[ExecutionID]*Execution),
		debug:              make(map[DebugSessionID]*DebugSession),
		releasedPlans:      make(map[PlanID]*Plan),
		releasedExecutions: make(map[ExecutionID]*Execution),
		releasedDebug:      make(map[DebugSessionID]*DebugSession),
	}
}

func (c *Connection) ID() ConnectionID {
	return c.id
}

func (c *Connection) Context() context.Context {
	return c.ctx
}

func (c *Connection) Close(ctx context.Context) error {
	if c.close.Begin() {
		go func() {
			c.close.Finish(c.settleClose())
		}()
	}

	return c.close.Wait(ctx)
}

func (c *Connection) settleClose() error {
	c.mu.Lock()
	c.closed = true
	c.cancel(context.Canceled)
	debugIDs := make([]DebugSessionID, 0, len(c.debug)+len(c.releasedDebug))
	for id := range c.debug {
		debugIDs = append(debugIDs, id)
	}
	for id := range c.releasedDebug {
		if _, active := c.debug[id]; !active {
			debugIDs = append(debugIDs, id)
		}
	}
	executionIDs := make([]ExecutionID, 0, len(c.executions)+len(c.releasedExecutions))
	for id := range c.executions {
		executionIDs = append(executionIDs, id)
	}
	for id := range c.releasedExecutions {
		if _, active := c.executions[id]; !active {
			executionIDs = append(executionIDs, id)
		}
	}
	planIDs := make([]PlanID, 0, len(c.plans)+len(c.releasedPlans))
	for id := range c.plans {
		planIDs = append(planIDs, id)
	}
	for id := range c.releasedPlans {
		if _, active := c.plans[id]; !active {
			planIDs = append(planIDs, id)
		}
	}
	c.mu.Unlock()

	var result error
	for _, id := range debugIDs {
		result = errors.Join(result, c.ReleaseDebugSession(context.Background(), id))
	}
	for _, id := range executionIDs {
		result = errors.Join(result, c.ReleaseExecution(context.Background(), id))
	}
	for _, id := range planIDs {
		result = errors.Join(result, c.ReleasePlan(context.Background(), id))
	}

	c.mu.Lock()
	clear(c.debug)
	clear(c.releasedDebug)
	clear(c.executions)
	clear(c.releasedExecutions)
	clear(c.plans)
	clear(c.releasedPlans)
	c.mu.Unlock()

	return result
}

func (c *Connection) ensureOpenLocked() error {
	if c.closed {
		return invalidState("connection is closed", context.Canceled)
	}

	return nil
}
