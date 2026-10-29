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

type (
	// Execution owns preparation, one output consumer, and output cleanup until release.
	Execution struct {
		mu          sync.Mutex
		id          ExecutionID
		store       *ResourceStore
		plan        *Plan
		session     *Session
		operation   func(context.Context) (api.Output, error)
		ctx         context.Context
		cancel      context.CancelCauseFunc
		options     []api.SessionOption
		state       wireexecution.State
		metadata    *api.Metadata
		failure     *failure.Failure
		events      *eventStream[wireexecution.Event]
		ready       chan struct{}
		delivery    chan outputDelivery
		claimed     bool
		prepareErr  error
		outcome     error
		cleanupErr  error
		done        chan struct{}
		release     lifecycle.Close
		constructed chan struct{}
	}

	outputDelivery struct {
		ctx     context.Context
		header  func(api.Metadata) error
		consume api.Consumer
	}

	// outputCallbackPanic distinguishes Wire callback defects from hosted panics.
	outputCallbackPanic struct{ value any }
)

func newExecution(store *ResourceStore, plan *Plan, session *Session, operation func(context.Context) (api.Output, error), options []api.SessionOption) *Execution {
	lifetime := store.ctx

	if session != nil {
		lifetime = session.ctx
	}

	ctx, cancel := context.WithCancelCause(lifetime)

	execution := &Execution{
		id: ExecutionID(uuid.NewString()), store: store, plan: plan, session: session,
		operation: operation, ctx: ctx, cancel: cancel, options: options,
		state:  wireexecution.StateRunning,
		events: newEventStream(store.limits.Watchers, cloneExecutionEvent, sequenceExecutionEvent),
		ready:  make(chan struct{}), delivery: make(chan outputDelivery), done: make(chan struct{}),
	}
	if plan != nil && session == nil && operation == nil {
		execution.constructed = make(chan struct{})
	}

	execution.publishLocked(false)

	return execution
}

// Release cancels and joins output cleanup before removing the handle. A canceled
// waiter never abandons committed cleanup.
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
	e.mu.Lock()
	err = e.cleanupErr
	e.mu.Unlock()
	e.events.close()
}

// Context ends on cancellation or release, independently of output settlement.
func (e *Execution) Context() context.Context { return e.ctx }

// ReadOutput claims the only delivery. Header and consumer run outside resource
// locks. The consumer must honor its context; it must not retain borrowed bytes.
// Cancellation stops waiting, while the execution retains callbacks and hosted
// cleanup until settlement. Callers must not inspect callback state on that path.
func (e *Execution) ReadOutput(ctx context.Context, header func(api.Metadata) error, consumer api.Consumer) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	e.mu.Lock()
	if e.claimed {
		e.mu.Unlock()

		return invalidState("execution output is no longer available", nil)
	}

	e.claimed = true
	e.mu.Unlock()
	operation, cancel := OperationContext(ctx, e.ctx)
	defer func() {
		if operation.Err() != nil {
			e.cancel(context.Cause(operation))
		}

		cancel()
	}()
	select {
	case <-e.ready:
	case <-operation.Done():
		return errors.Join(operation.Err(), context.Cause(operation))
	}

	e.mu.Lock()
	err := e.prepareErr
	running := e.state == wireexecution.StateRunning
	e.mu.Unlock()

	if err != nil {
		return err
	}

	if !running {
		return invalidState("execution output is no longer available", nil)
	}

	select {
	case e.delivery <- outputDelivery{ctx: operation, header: header, consume: consumer}:
	case <-e.done:
	case <-operation.Done():
		return errors.Join(operation.Err(), context.Cause(operation))
	}

	select {
	case <-e.done:
	case <-operation.Done():
		return errors.Join(operation.Err(), context.Cause(operation))
	}

	e.mu.Lock()
	err = e.outcome
	e.mu.Unlock()

	return err
}

func (e *Execution) run() {
	var temporary api.Session
	var output api.Output
	var err error

	if e.operation != nil {
		output, err = e.operation(e.ctx)
	} else {
		temporary, err = panicboundary.Call(func() (api.Session, error) { return e.plan.plan.NewSession(e.ctx, e.options...) })

		if e.constructed != nil {
			close(e.constructed)
		}

		if err != nil {
			err = internalError(err)
		}

		if err == nil && isNil(temporary) {
			err = internalError(errors.New("runtime returned no session"))
		}

		if err == nil {
			output, err = panicboundary.Call(func() (api.Output, error) { return temporary.Run(e.ctx) })
		}
	}

	if err == nil && isNil(output) {
		err = internalError(errors.New("runtime returned no output"))
	}

	var metadata api.Metadata

	if err == nil {
		metadata, err = panicboundary.Call(func() (api.Metadata, error) { return output.Metadata(), nil })
		if err == nil && metadata.LengthKnown && metadata.Length < 0 {
			err = internalError(errors.New("runtime returned invalid output metadata"))
		}
	}

	if err != nil {
		cleanup := e.closeOutput(output, temporary)
		e.mu.Lock()
		e.prepareErr = errors.Join(err, cleanup)
		e.mu.Unlock()
		e.finish(errors.Join(err, cleanup), cleanup)
		close(e.ready)

		return
	}

	e.mu.Lock()
	e.metadata = &metadata
	e.mu.Unlock()
	close(e.ready)
	select {
	case delivery := <-e.delivery:
		err = delivery.header(metadata)
		if err == nil {
			err = panicboundary.Do(func() error {
				return output.Consume(delivery.ctx, func(ctx context.Context, chunk []byte) (result error) {
					completed := false
					defer func() {
						if !completed {
							panic(outputCallbackPanic{value: recover()})
						}
					}()
					result = delivery.consume(ctx, chunk)
					completed = true

					return result
				})
			})
		}

	case <-e.ctx.Done():
		err = context.Cause(e.ctx)
	}

	cleanup := e.closeOutput(output, temporary)

	var boundary *panicboundary.Error
	if errors.As(err, &boundary) {
		if callback, ok := boundary.Value.(outputCallbackPanic); ok {
			e.finish(internalError(errors.New("Wire output callback panicked")), cleanup)
			panic(callback.value)
		}
	}

	if err == nil && cleanup != nil {
		err = internalError(cleanup)
	}

	e.finish(errors.Join(err, cleanup), cleanup)
}

func (e *Execution) closeOutput(output api.Output, temporary api.Session) error {
	var err error

	if !isNil(output) {
		err = panicboundary.Do(output.Close)
	}

	if !isNil(temporary) {
		err = errors.Join(err, closeAPISession(temporary))
	}

	return err
}

func (e *Execution) finish(err, cleanup error) {
	var boundary *panicboundary.Error
	if errors.As(err, &boundary) && e.session != nil {
		e.session.poisoned.Store(true)
	}

	category := failure.CategoryExecution

	var domain *DomainError
	if boundary != nil || errors.As(err, &domain) && domain.Kind == ErrorKindInternal {
		category = failure.CategoryInternalRuntime
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state != wireexecution.StateRunning {
		return
	}

	e.outcome, e.cleanupErr = err, cleanup
	if err != nil {
		e.failure = failureFromError(category, err)
	}

	switch {
	case errors.Is(err, context.Canceled) || errors.Is(context.Cause(e.ctx), context.Canceled):
		e.state = wireexecution.StateCancelled
	case err != nil:
		e.state = wireexecution.StateFailed
	default:
		e.state = wireexecution.StateCompleted
	}

	e.publishLocked(true)
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

// Snapshot returns execution state with metadata and diagnostics detached.
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
		State:    e.state,
		Metadata: e.metadata,
		Failure:  e.failure,
	})
}

func (e *Execution) publishLocked(terminal bool) {
	e.events.publish(wireexecution.Event{Snapshot: e.snapshotLocked()}, terminal)

	if terminal {
		close(e.done)
	}
}
