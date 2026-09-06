package core

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"

	"github.com/MontFerret/api"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/lifecycle"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

// Execution owns one asynchronous run and retains its terminal result until release.
type Execution struct {
	mu        sync.Mutex
	id        ExecutionID
	store     *ResourceStore
	plan      *Plan
	session   *Session
	operation func(context.Context) (api.Output, error)
	ctx       context.Context
	cancel    context.CancelCauseFunc
	options   []api.SessionOption
	state     wireexecution.State
	output    *api.Output
	failure   *failure.Failure
	events    *eventStream[wireexecution.Event]
	done      chan struct{}
	release   lifecycle.Close
}

func newExecution(store *ResourceStore, plan *Plan, session *Session, operation func(context.Context) (api.Output, error), options []api.SessionOption) *Execution {
	lifetime := store.ctx

	if session != nil {
		lifetime = session.ctx
	}

	ctx, cancel := context.WithCancelCause(lifetime)
	execution := &Execution{
		id:        ExecutionID(uuid.NewString()),
		store:     store,
		plan:      plan,
		session:   session,
		operation: operation,
		options:   options,
		ctx:       ctx,
		cancel:    cancel,
		state:     wireexecution.StateRunning,
		events:    newEventStream(store.limits.Watchers, cloneExecutionEvent, sequenceExecutionEvent),
		done:      make(chan struct{}),
	}
	execution.publishLocked(false)

	return execution
}

// Release cancels and joins the run, closes its event stream, and removes its handle.
// Caller cancellation stops waiting without abandoning teardown.
func (e *Execution) Release(ctx context.Context) error {
	e.store.mu.Lock()
	started := e.release.Begin()
	e.store.mu.Unlock()

	if started {
		go e.settleRelease()
	}

	return e.release.Wait(ctx)
}

func (e *Execution) settleRelease() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("execution release panicked")))
		}

		e.store.removeExecution(e)
		e.release.Finish(err)
	}()

	e.cancel(context.Canceled)
	<-e.done
	e.events.close()
}

func (e *Execution) run() {
	if e.operation != nil {
		e.runOperation()

		return
	}

	session, err := panicboundary.Call(func() (api.Session, error) {
		return e.plan.plan.NewSession(e.ctx, e.options...)
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

	result := &api.Output{
		ContentType: output.ContentType,
		Content:     append([]byte(nil), output.Content...),
	}

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
	result := &api.Output{
		ContentType: output.ContentType,
		Content:     append([]byte(nil), output.Content...),
	}
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

// Cancel requests cancellation and returns the current snapshot without waiting for termination.
func (e *Execution) Cancel() wireexecution.Snapshot {
	e.cancel(context.Canceled)

	return e.Snapshot()
}

// ID identifies this run within its logical connection.
func (e *Execution) ID() ExecutionID {
	return e.id
}

// Snapshot returns execution state with mutable output and diagnostics detached.
func (e *Execution) Snapshot() wireexecution.Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.snapshotLocked()
}

// Watch reserves a bounded subscription with the current snapshot.
// The caller must cancel the subscription to release its watcher slot.
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

func (e *Execution) snapshotLocked() wireexecution.Snapshot {
	return cloneExecutionSnapshot(wireexecution.Snapshot{
		State:   e.state,
		Output:  e.output,
		Failure: e.failure,
	})
}

func (e *Execution) publishLocked(terminal bool) {
	e.events.publish(wireexecution.Event{Snapshot: e.snapshotLocked()}, terminal)

	if terminal {
		close(e.done)
	}
}
