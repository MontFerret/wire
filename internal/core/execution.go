package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	Execution struct {
		mu          sync.Mutex
		id          ExecutionID
		owner       ConnectionID
		planID      PlanID
		plan        api.Plan
		ctx         context.Context
		cancel      context.CancelCauseFunc
		parameters  map[string]any
		contentType string
		state       ExecutionState
		output      *Output
		failure     *Failure
		events      *eventStream[ExecutionEvent]
		done        chan struct{}
		close       lifecycle.Close
		release     lifecycle.Close
	}

	ExecuteInput struct {
		PlanID            PlanID
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
		state:       ExecutionRunning,
		events:      newEventStream[ExecutionEvent](maxWatchers),
		done:        make(chan struct{}),
	}
	execution.publishLocked(ExecutionEventStarted, false)

	return execution
}

func (e *Execution) run() {
	options := []api.SessionOption{api.WithParams(cloneParameters(e.parameters))}
	if e.contentType != "" {
		options = append(options, api.WithOutputContentType(e.contentType))
	}

	session, err, _ := openAPISession(e.ctx, e.plan, options)
	if err != nil {
		if !isNil(session) {
			err = errors.Join(err, closeAPISession(session))
		}

		e.finish(nil, err, ErrorInternal)

		return
	}

	if isNil(session) {
		e.finish(nil, errors.New("runtime returned no session"), ErrorInternal)

		return
	}

	output, runErr, panicked := runAPISession(e.ctx, session)
	closeErr := closeAPISession(session)
	err = errors.Join(runErr, closeErr)

	var result *Output
	if !panicked {
		result = &Output{ContentType: output.ContentType, Content: append([]byte(nil), output.Content...)}
	}

	category := ErrorInternal
	if runErr != nil && !panicked {
		category = ErrorExecution
	}

	e.finish(result, err, category)
}

func (e *Execution) finish(output *Output, err error, category ErrorCategory) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state != ExecutionRunning {
		return
	}

	e.output = output
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(context.Cause(e.ctx), context.Canceled):
		e.state = ExecutionCancelled
		e.publishLocked(ExecutionEventCancelled, true)
	case err != nil:
		e.state = ExecutionFailed
		e.failure = failureFromError(category)
		e.publishLocked(ExecutionEventFailed, true)
	default:
		e.state = ExecutionCompleted
		e.publishLocked(ExecutionEventCompleted, true)
	}
}

func (e *Execution) Cancel() ExecutionSnapshot {
	e.cancel(context.Canceled)

	return e.Snapshot()
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

func (e *Execution) Snapshot() ExecutionSnapshot {
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

func (e *Execution) snapshotLocked() ExecutionSnapshot {
	result := ExecutionSnapshot{ID: e.id, PlanID: e.planID, State: e.state}
	if e.output != nil {
		result.Output = &Output{ContentType: e.output.ContentType, Content: append([]byte(nil), e.output.Content...)}
	}

	if e.failure != nil {
		result.Failure = &Failure{Category: e.failure.Category, Message: e.failure.Message}
	}

	return result
}

func (e *Execution) publishLocked(kind ExecutionEventKind, terminal bool) {
	e.events.publish(ExecutionEvent{
		Execution: e.id,
		Kind:      kind,
		Snapshot:  e.snapshotLocked(),
	}, terminal)

	if terminal {
		close(e.done)
	}
}
