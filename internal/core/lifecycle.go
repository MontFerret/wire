package core

import (
	"context"
	"errors"
)

// Lifecycle coordinates teardown that spans registry and resource boundaries.
type Lifecycle struct {
	connections *ConnectionRegistry
	plans       *PlanRegistry
	executions  *ExecutionRegistry
	sessions    *DebugSessionRegistry
}

func NewLifecycle(
	connections *ConnectionRegistry,
	plans *PlanRegistry,
	executions *ExecutionRegistry,
	sessions *DebugSessionRegistry,
) *Lifecycle {
	return &Lifecycle{
		connections: connections,
		plans:       plans,
		executions:  executions,
		sessions:    sessions,
	}
}

func (l *Lifecycle) ReleaseExecution(ctx *Context, id ExecutionID) error {
	return l.releaseExecution(ctx, ctx.connectionID(), id)
}

func (l *Lifecycle) releaseExecution(waiter context.Context, owner ConnectionID, id ExecutionID) error {
	execution, started, err := l.executions.beginClose(owner, id)
	if err != nil {
		return err
	}

	if started {
		go l.settleExecution(execution)
	}

	return execution.release.Wait(waiter)
}

func (l *Lifecycle) settleExecution(execution *Execution) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("execution release panicked")))
		}

		l.executions.remove(execution)
		execution.release.Finish(err)
	}()

	err = execution.Close(context.Background())
}

func (l *Lifecycle) ReleaseDebugSession(ctx *Context, id DebugSessionID) error {
	return l.releaseDebugSession(ctx, ctx.connectionID(), id)
}

func (l *Lifecycle) releaseDebugSession(waiter context.Context, owner ConnectionID, id DebugSessionID) error {
	session, started, err := l.sessions.beginClose(owner, id)
	if err != nil {
		return err
	}

	if started {
		go l.settleDebugSession(session)
	}

	return session.release.Wait(waiter)
}

func (l *Lifecycle) settleDebugSession(session *DebugSession) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("debug session release panicked")))
		}

		l.sessions.remove(session)
		session.release.Finish(err)
	}()

	err = session.Close(context.Background())
}

func (l *Lifecycle) ReleasePlan(ctx *Context, id PlanID) error {
	return l.releasePlan(ctx, ctx.connectionID(), id)
}

func (l *Lifecycle) releasePlan(waiter context.Context, owner ConnectionID, id PlanID) error {
	plan, started, err := l.plans.beginClose(owner, id)
	if err != nil {
		return err
	}

	if started {
		go l.settlePlan(plan)
	}

	return plan.waitClose(waiter)
}

func (l *Lifecycle) settlePlan(plan *Plan) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("plan cleanup panicked")))
		}

		l.plans.remove(plan)
		plan.finishClose(err)
	}()

	plan.waitChildCreations()

	for _, id := range l.executions.listByPlan(plan.owner, plan.id) {
		releaseErr := l.releaseExecution(context.Background(), plan.owner, id)
		err = errors.Join(err, ignoreMissingResource(releaseErr, ErrorExecutionNotFound))
	}

	for _, id := range l.sessions.listByPlan(plan.owner, plan.id) {
		releaseErr := l.releaseDebugSession(context.Background(), plan.owner, id)
		err = errors.Join(err, ignoreMissingResource(releaseErr, ErrorDebugSessionNotFound))
	}

	err = errors.Join(err, closeAPIPlan(plan.plan))
}

func (l *Lifecycle) CloseConnection(ctx context.Context, id ConnectionID) error {
	connection, started, err := l.connections.beginClose(id)
	if err != nil {
		return err
	}

	if started {
		go l.settleConnection(connection)
	}

	return connection.waitClose(ctx)
}

func (l *Lifecycle) settleConnection(connection *Connection) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("logical connection cleanup panicked")))
		}

		l.connections.remove(connection.ID(), connection)
		connection.finishClose(err)
	}()

	connection.waitOperations()
	owner := connection.ID()

	for _, id := range l.executions.listByOwner(owner) {
		releaseErr := l.releaseExecution(context.Background(), owner, id)
		err = errors.Join(err, ignoreMissingResource(releaseErr, ErrorExecutionNotFound))
	}

	for _, id := range l.sessions.listByOwner(owner) {
		releaseErr := l.releaseDebugSession(context.Background(), owner, id)
		err = errors.Join(err, ignoreMissingResource(releaseErr, ErrorDebugSessionNotFound))
	}

	for _, id := range l.plans.listByOwner(owner) {
		releaseErr := l.releasePlan(context.Background(), owner, id)
		err = errors.Join(err, ignoreMissingResource(releaseErr, ErrorPlanNotFound))
	}
}

func (l *Lifecycle) Close(ctx context.Context) error {
	ids := l.connections.beginShutdown()
	var result error
	for _, id := range ids {
		result = errors.Join(result, l.CloseConnection(ctx, id))
	}

	return result
}
