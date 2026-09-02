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
	var session api.Session
	closeAttempted := false

	defer func() {
		if recover() == nil {
			return
		}

		if !isNil(session) && !closeAttempted {
			closeAttempted = true
			_ = closeAPISession(session)
		}

		e.finish(nil, errors.New("runtime execution panicked"), ErrorInternal)
	}()

	options := []api.SessionOption{api.WithParams(cloneParameters(e.parameters))}
	if e.contentType != "" {
		options = append(options, api.WithOutputContentType(e.contentType))
	}

	var err error
	session, err = e.plan.NewSession(e.ctx, options...)
	if err != nil {
		if !isNil(session) {
			closeAttempted = true
			err = errors.Join(err, closeAPISession(session))
			session = nil
		}

		e.finish(nil, err, ErrorInternal)

		return
	}

	if isNil(session) {
		e.finish(nil, errors.New("runtime returned no session"), ErrorInternal)

		return
	}

	output, runErr := session.Run(e.ctx)
	closeAttempted = true
	closeErr := closeAPISession(session)
	session = nil
	err = errors.Join(runErr, closeErr)
	result := &Output{ContentType: output.ContentType, Content: append([]byte(nil), output.Content...)}
	category := ErrorInternal
	if runErr != nil {
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
