package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
)

// Connection is the logical ownership scope established by RuntimeService.Connect.
type Connection struct {
	mu            sync.Mutex
	id            ConnectionID
	ctx           context.Context
	cancel        context.CancelCauseFunc
	plans         *planStore
	executions    *executionStore
	debugSessions *debugSessionStore
	operations    sync.WaitGroup
	closed        bool
	close         lifecycle.Close
}

func newConnection(id ConnectionID, runtime api.Runtime, limits Limits) *Connection {
	ctx, cancel := context.WithCancelCause(context.Background())
	connection := &Connection{
		id:     id,
		ctx:    ctx,
		cancel: cancel,
	}

	connection.plans = newPlanStore(connection, runtime, limits.MaxPlansPerConnection)
	connection.executions = newExecutionStore(connection, limits.MaxExecutionsPerConnection, limits.MaxWatchersPerResource)
	connection.debugSessions = newDebugSessionStore(
		connection,
		limits.MaxDebugSessionsPerConnection,
		limits.MaxWatchersPerResource,
		limits.MaxBreakpointsPerDebugSession,
	)
	connection.plans.attachChildren(connection.executions, connection.debugSessions)

	return connection
}

func (c *Connection) ID() ConnectionID {
	return c.id
}

func (c *Connection) Context() context.Context {
	return c.ctx
}

func (c *Connection) Close(ctx context.Context) error {
	if c.close.Begin() {
		c.mu.Lock()
		c.closed = true
		c.cancel(context.Canceled)
		c.mu.Unlock()

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
	c.operations.Wait()

	var result error
	result = errors.Join(result, c.debugSessions.closeAll())
	result = errors.Join(result, c.executions.closeAll())
	result = errors.Join(result, c.plans.closeAll())

	return result
}

// beginOperation registers creation before Close can begin waiting. Callers
// must finish the operation after either committing or rolling back store state.
func (c *Connection) beginOperation() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureOpenLocked(); err != nil {
		return err
	}

	c.operations.Add(1)

	return nil
}

func (c *Connection) finishOperation() {
	c.operations.Done()
}

// commitCreation linearizes publication against connection shutdown. The
// callback may take store and resource locks, but must not call external code.
func (c *Connection) commitCreation(commit func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureOpenLocked(); err != nil {
		return err
	}

	return commit()
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

func (c *Connection) ensureOpenLocked() error {
	if c.closed {
		return invalidState("connection is closed", context.Canceled)
	}

	return nil
}
