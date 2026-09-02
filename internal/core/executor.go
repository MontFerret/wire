package core

import (
	"context"

	"github.com/google/uuid"
)

// Executor owns asynchronous execution creation and ownership-checked lookup.
type Executor struct {
	plans      *PlanRegistry
	executions *ExecutionRegistry
}

func NewExecutor(plans *PlanRegistry, executions *ExecutionRegistry) *Executor {
	return &Executor{plans: plans, executions: executions}
}

func (e *Executor) Execute(ctx *Context, input ExecuteInput) (ExecutionSnapshot, error) {
	connection := ctx.Connection()

	if err := connection.beginOperation(); err != nil {
		return ExecutionSnapshot{}, err
	}
	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return ExecutionSnapshot{}, err
	}

	owner := connection.ID()
	if err := e.executions.reserve(owner); err != nil {
		return ExecutionSnapshot{}, err
	}

	reserved := true
	defer func() {
		if reserved {
			e.executions.rollback(owner)
		}
	}()

	plan, err := e.plans.beginChild(owner, input.PlanID, false)
	if err != nil {
		return ExecutionSnapshot{}, err
	}

	defer plan.finishChildCreation()

	executionCtx, cancel := context.WithCancelCause(connection.Context())
	created := &Execution{
		id:          ExecutionID(uuid.NewString()),
		owner:       owner,
		planID:      plan.id,
		plan:        plan.plan,
		ctx:         executionCtx,
		cancel:      cancel,
		parameters:  cloneParameters(input.Parameters),
		contentType: input.OutputContentType,
		maxWatchers: e.executions.maxWatchers,
		state:       ExecutionRunning,
		watchers:    make(map[uint64]*executionWatcher),
		done:        make(chan struct{}),
	}

	created.publishLocked(ExecutionEventStarted, false)

	err = e.plans.commitChild(owner, input.PlanID, plan, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		return e.executions.commit(created)
	})
	if err != nil {
		cancel(context.Canceled)

		return ExecutionSnapshot{}, err
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
