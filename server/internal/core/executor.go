package core

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/panicboundary"
	"github.com/google/uuid"
)

type (
	// Executor owns durable session and asynchronous execution creation.
	Executor struct {
		runtime    api.Runtime
		plans      *PlanRegistry
		sessions   *SessionRegistry
		executions *ExecutionRegistry
	}

	RunInput struct {
		Source            api.Source
		Parameters        map[string]any
		OutputContentType string
	}

	CreateSessionInput struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}
)

func NewExecutor(
	runtime api.Runtime,
	plans *PlanRegistry,
	sessions *SessionRegistry,
	executions *ExecutionRegistry,
) *Executor {
	return &Executor{runtime: runtime, plans: plans, sessions: sessions, executions: executions}
}

func (e *Executor) Execute(ctx *Context, input ExecuteInput) (ExecutionRecord, error) {
	connection := ctx.Connection()

	if err := connection.beginOperation(); err != nil {
		return ExecutionRecord{}, err
	}
	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return ExecutionRecord{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return ExecutionRecord{}, err
	}

	owner := connection.ID()
	if err := e.executions.reserve(owner); err != nil {
		return ExecutionRecord{}, err
	}

	reserved := true
	defer func() {
		if reserved {
			e.executions.rollback(owner)
		}
	}()

	plan, err := e.plans.beginChild(owner, input.PlanID, false)
	if err != nil {
		return ExecutionRecord{}, err
	}

	defer plan.finishChildCreation()

	executionCtx, cancel := context.WithCancelCause(connection.Context())
	created := newExecution(
		ExecutionID(uuid.NewString()),
		owner,
		plan.id,
		plan.plan,
		executionCtx,
		cancel,
		input,
		e.executions.maxWatchers,
	)

	err = e.plans.commitChild(owner, input.PlanID, plan, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		return e.executions.commit(created)
	})
	if err != nil {
		cancel(context.Canceled)

		return ExecutionRecord{}, err
	}

	reserved = false

	go created.run()

	return created.Snapshot(), nil
}

func (e *Executor) Execution(ctx *Context, id ExecutionID) (*Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return e.executions.get(ctx.connectionID(), id)
}

func (e *Executor) CreateSession(ctx *Context, input CreateSessionInput) (SessionID, error) {
	connection := ctx.Connection()
	if err := connection.beginOperation(); err != nil {
		return "", err
	}
	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return "", err
	}

	owner := connection.ID()
	if err := e.sessions.reserve(owner); err != nil {
		return "", err
	}

	reserved := true
	defer func() {
		if reserved {
			e.sessions.rollback(owner)
		}
	}()

	plan, err := e.plans.beginChild(owner, input.PlanID, false)
	if err != nil {
		return "", err
	}
	defer plan.finishChildCreation()

	hostedSession, err := panicboundary.Call(func() (api.Session, error) {
		return plan.plan.NewSession(ctx, apiSessionOptions(input.Parameters, input.OutputContentType)...)
	})
	if err != nil {
		var closeErr error
		if !isNil(hostedSession) {
			closeErr = closeAPISession(hostedSession)
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", errors.Join(ctxErr, closeErr)
		}

		return "", errors.Join(internalError(err), closeErr)
	}

	if isNil(hostedSession) {
		return "", internalError(errors.New("runtime returned no session"))
	}

	if err := ctx.Err(); err != nil {
		return "", errors.Join(err, closeAPISession(hostedSession))
	}

	sessionCtx, cancel := context.WithCancelCause(connection.Context())
	created := newSession(
		SessionID(uuid.NewString()),
		owner,
		plan.id,
		hostedSession,
		sessionCtx,
		cancel,
	)

	committed := false
	err = e.plans.commitChild(owner, input.PlanID, plan, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		committed = true

		return e.sessions.commit(created)
	})
	if committed {
		reserved = false
	}

	if err != nil {
		cancel(context.Canceled)

		return "", errors.Join(err, closeAPISession(hostedSession))
	}

	return created.id, nil
}

func (e *Executor) RunSession(ctx *Context, id SessionID) (ExecutionRecord, error) {
	connection := ctx.Connection()
	if err := connection.beginOperation(); err != nil {
		return ExecutionRecord{}, err
	}
	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return ExecutionRecord{}, err
	}

	owner := connection.ID()
	if err := e.executions.reserve(owner); err != nil {
		return ExecutionRecord{}, err
	}

	reserved := true
	defer func() {
		if reserved {
			e.executions.rollback(owner)
		}
	}()

	candidate, err := e.sessions.get(owner, id)
	if err != nil {
		return ExecutionRecord{}, err
	}

	plan, err := e.plans.beginChild(owner, candidate.planID, false)
	if err != nil {
		return ExecutionRecord{}, err
	}
	defer plan.finishChildCreation()

	executionID := ExecutionID(uuid.NewString())
	session, err := e.sessions.beginExecution(owner, id, executionID)
	if err != nil {
		return ExecutionRecord{}, err
	}
	defer session.finishExecutionCreation()

	committed := false
	defer func() {
		if !committed {
			session.finishExecution(executionID)
		}
	}()

	executionCtx, cancel := context.WithCancelCause(session.Context())
	created := newOperationExecution(
		executionID,
		owner,
		plan.id,
		session.id,
		executionCtx,
		cancel,
		session.Run,
		e.executions.maxWatchers,
	)

	registryCommitted := false
	err = e.plans.commitChild(owner, plan.id, plan, func() error {
		return e.sessions.commitExecution(owner, id, session, executionID, func() error {
			if err := ctx.Err(); err != nil {
				return err
			}

			registryCommitted = true

			return e.executions.commit(created)
		})
	})
	if registryCommitted {
		reserved = false
	}

	if err != nil {
		cancel(context.Canceled)

		return ExecutionRecord{}, err
	}

	committed = true
	snapshot := created.Snapshot()
	go created.run()

	return snapshot, nil
}

func (e *Executor) Run(ctx *Context, input RunInput) (ExecutionRecord, error) {
	connection := ctx.Connection()
	if err := connection.beginOperation(); err != nil {
		return ExecutionRecord{}, err
	}
	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return ExecutionRecord{}, err
	}

	if input.Source.Content == "" {
		return ExecutionRecord{}, invalidRequest("source content is required")
	}

	if input.Source.Name == "" {
		input.Source.Name = "anonymous"
	}

	owner := connection.ID()
	if err := e.executions.reserve(owner); err != nil {
		return ExecutionRecord{}, err
	}

	reserved := true
	defer func() {
		if reserved {
			e.executions.rollback(owner)
		}
	}()

	executionCtx, cancel := context.WithCancelCause(connection.Context())
	created := newOperationExecution(
		ExecutionID(uuid.NewString()),
		owner,
		"",
		"",
		executionCtx,
		cancel,
		func(runCtx context.Context) (api.Output, error) {
			return e.run(runCtx, input)
		},
		e.executions.maxWatchers,
	)

	if err := ctx.Err(); err != nil {
		cancel(context.Canceled)

		return ExecutionRecord{}, err
	}

	err := e.executions.commit(created)
	reserved = false
	if err != nil {
		cancel(context.Canceled)

		return ExecutionRecord{}, err
	}

	// Capture the promised running response before fast hosted work can finish.
	snapshot := created.Snapshot()
	go created.run()

	return snapshot, nil
}

func (e *Executor) run(ctx context.Context, input RunInput) (api.Output, error) {
	output, err := panicboundary.Call(func() (api.Output, error) {
		return e.runtime.Run(ctx, input.Source, apiSessionOptions(input.Parameters, input.OutputContentType)...)
	})

	return output, runtimePanicError("run hosted runtime", err)
}
