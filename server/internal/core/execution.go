package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/lifecycle"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

type (
	Execution struct {
		mu          sync.Mutex
		id          ExecutionID
		owner       ConnectionID
		planID      PlanID
		sessionID   SessionID
		plan        api.Plan
		operation   func(context.Context) (api.Output, error)
		ctx         context.Context
		cancel      context.CancelCauseFunc
		parameters  map[string]any
		contentType string
		state       wireexecution.State
		output      *api.Output
		failure     *failure.Failure
		events      *eventStream[wireexecution.Event]
		done        chan struct{}
		close       lifecycle.Close
		release     lifecycle.Close
	}

	ExecuteInput struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	RunRuntimeInput struct {
		Source            api.Source
		Parameters        map[string]any
		OutputContentType string
	}
)

func newExecution(
	id ExecutionID,
	owner ConnectionID,
	planID PlanID,
	plan api.Plan,
	ctx context.Context,
	cancel context.CancelCauseFunc,
	input ExecuteInput,
	maxWatchers int,
) *Execution {
	execution := &Execution{
		id:          id,
		owner:       owner,
		planID:      planID,
		plan:        plan,
		ctx:         ctx,
		cancel:      cancel,
		parameters:  cloneParameters(input.Parameters),
		contentType: input.OutputContentType,
		state:       wireexecution.StateRunning,
		events:      newEventStream(maxWatchers, cloneExecutionEvent, sequenceExecutionEvent),
		done:        make(chan struct{}),
	}
	execution.publishLocked(false)

	return execution
}

func newOperationExecution(
	id ExecutionID,
	owner ConnectionID,
	planID PlanID,
	sessionID SessionID,
	ctx context.Context,
	cancel context.CancelCauseFunc,
	operation func(context.Context) (api.Output, error),
	maxWatchers int,
) *Execution {
	execution := &Execution{
		id:        id,
		owner:     owner,
		planID:    planID,
		sessionID: sessionID,
		operation: operation,
		ctx:       ctx,
		cancel:    cancel,
		state:     wireexecution.StateRunning,
		events:    newEventStream(maxWatchers, cloneExecutionEvent, sequenceExecutionEvent),
		done:      make(chan struct{}),
	}
	execution.publishLocked(false)

	return execution
}

func (e *Execution) run() {
	if e.operation != nil {
		e.runOperation()

		return
	}

	session, err := panicboundary.Call(func() (api.Session, error) {
		return e.plan.NewSession(e.ctx, apiSessionOptions(e.parameters, e.contentType)...)
	})
	if err != nil {
		if !isNil(session) {
			err = errors.Join(err, closeAPISession(session))
		}

		e.finish(nil, err, failure.CategoryInternalRuntime)

		return
	}

	if isNil(session) {
		e.finish(nil, errors.New("runtime returned no session"), failure.CategoryInternalRuntime)

		return
	}

	output, runErr := panicboundary.Call(func() (api.Output, error) {
		return session.Run(e.ctx)
	})
	closeErr := closeAPISession(session)
	err = errors.Join(runErr, closeErr)

	result := &api.Output{ContentType: output.ContentType, Content: append([]byte(nil), output.Content...)}
	var panicErr *panicboundary.Error
	if errors.As(runErr, &panicErr) {
		result = nil
	}

	category := failure.CategoryInternalRuntime
	if runErr != nil && panicErr == nil {
		category = failure.CategoryExecution
	}

	e.finish(result, err, category)
}

func (e *Execution) runOperation() {
	output, runErr := e.operation(e.ctx)
	result := &api.Output{ContentType: output.ContentType, Content: append([]byte(nil), output.Content...)}
	category := failure.CategoryExecution

	var domain *DomainError
	if errors.As(runErr, &domain) && domain.Kind == ErrorKindInternal {
		category = failure.CategoryInternalRuntime
		result = nil
	}

	e.finish(result, runErr, category)
}

func (e *Execution) finish(output *api.Output, err error, category failure.Category) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state != wireexecution.StateRunning {
		return
	}

	e.output = output
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(context.Cause(e.ctx), context.Canceled):
		e.state = wireexecution.StateCancelled
		e.publishLocked(true)
	case err != nil:
		e.state = wireexecution.StateFailed
		e.failure = failureFromError(category, err)
		e.publishLocked(true)
	default:
		e.state = wireexecution.StateCompleted
		e.publishLocked(true)
	}
}

func (e *Execution) Cancel() ExecutionRecord {
	e.cancel(context.Canceled)

	return e.Snapshot()
}

func (e *Execution) ID() ExecutionID {
	return e.id
}

func (e *Execution) Close(ctx context.Context) error {
	if e.close.Begin() {
		go e.settleClose()
	}

	return e.close.Wait(ctx)
}

func (e *Execution) settleClose() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("execution cleanup panicked")))
		}

		e.close.Finish(err)
	}()

	e.cancel(context.Canceled)
	<-e.done
	e.events.close()
}

func (e *Execution) Snapshot() ExecutionRecord {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.snapshotLocked()
}

func (e *Execution) Watch() (ExecutionSubscription, error) {
	subscription, err := e.events.subscribe()
	if err != nil {
		return ExecutionSubscription{}, resourceExhausted("execution watcher limit reached")
	}

	return ExecutionSubscription{
		Current: subscription.current,
		Events:  subscription.events,
		Errors:  subscription.errors,
		Cancel:  subscription.cancel,
	}, nil
}

func (e *Execution) snapshotLocked() ExecutionRecord {
	return ExecutionRecord{
		ID: e.id,
		Snapshot: cloneExecutionSnapshot(wireexecution.Snapshot{
			State:   e.state,
			Output:  e.output,
			Failure: e.failure,
		}),
	}
}

func (e *Execution) publishLocked(terminal bool) {
	e.events.publish(wireexecution.Event{Snapshot: e.snapshotLocked().Snapshot}, terminal)

	if terminal {
		close(e.done)
	}
}
