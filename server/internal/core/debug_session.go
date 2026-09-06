package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/lifecycle"
	"github.com/MontFerret/wire/server/internal/panicboundary"
	"github.com/google/uuid"
)

type DebugSession struct {
	// operationMu serializes state-dependent operations and command commits.
	// The active runtime resume and close paths intentionally do not hold it
	// so pause and cancellation can reach the hosted debugger.
	operationMu sync.Mutex
	// stateMu protects only Wire-visible state and never spans a runtime call.
	stateMu     sync.Mutex
	id          DebugSessionID
	plan        *Plan
	session     debugger.Session
	ctx         context.Context
	cancel      context.CancelCauseFunc
	state       debugSessionState
	breakpoints *breakpointSet
	events      *eventStream[wiredebugger.Event]
	close       lifecycle.Close
	release     lifecycle.Close
}

func newDebugSession(plan *Plan, hosted debugger.Session) *DebugSession {
	ctx, cancel := context.WithCancelCause(plan.store.ctx)
	session := &DebugSession{
		id:          DebugSessionID(uuid.NewString()),
		plan:        plan,
		session:     hosted,
		ctx:         ctx,
		cancel:      cancel,
		state:       debugSessionState{status: wiredebugger.StateCreated},
		breakpoints: newBreakpointSet(plan.store.limits.Breakpoints),
		events:      newEventStream(plan.store.limits.Watchers, cloneDebugEvent, sequenceDebugEvent),
	}
	session.publishLocked(wiredebugger.EventCreated, false)

	return session
}

func (d *DebugSession) Release(ctx context.Context) error {
	d.plan.store.mu.Lock()
	started := d.release.Begin()
	d.plan.store.mu.Unlock()
	if started {
		go d.settleRelease()
	}

	return d.release.Wait(ctx)
}

func (d *DebugSession) settleRelease() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("debug session release panicked")))
		}

		d.plan.store.removeDebugSession(d)

		d.release.Finish(err)
	}()

	err = d.Close(context.Background())
}

func (d *DebugSession) Close(ctx context.Context) error {
	d.beginClose()

	return d.close.Wait(ctx)
}

func (d *DebugSession) ID() DebugSessionID {
	return d.id
}

func (d *DebugSession) Stop(ctx context.Context) (wiredebugger.Snapshot, error) {
	snapshot := d.Snapshot()
	if !snapshot.State.Terminal() {
		if err := d.Close(ctx); err != nil {
			return wiredebugger.Snapshot{}, err
		}

		snapshot = d.Snapshot()
	}

	return snapshot, nil
}

func (d *DebugSession) Pause(ctx context.Context) (wiredebugger.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return wiredebugger.Snapshot{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	d.stateMu.Lock()
	if d.state.status != wiredebugger.StateRunning {
		d.stateMu.Unlock()

		return wiredebugger.Snapshot{}, invalidState("debug session is not running", nil)
	}

	d.stateMu.Unlock()

	if err := panicboundary.Do(d.session.Pause); err != nil {
		if panicErr := d.poisonAfterRuntimePanic("pause runtime debugger", err); panicErr != nil {
			return wiredebugger.Snapshot{}, panicErr
		}

		return wiredebugger.Snapshot{}, invalidState("pause failed", err)
	}

	return d.Snapshot(), nil
}

func (d *DebugSession) SetBreakpoint(
	ctx context.Context,
	location source.Location,
) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(ctx, location, debugger.BreakpointOptions{
		BindingMode: debugger.BreakpointBindNextExecutableInSource,
	})
}

func (d *DebugSession) SetBreakpointAt(
	ctx context.Context,
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	if err := ctx.Err(); err != nil {
		return debugger.Breakpoint{}, err
	}

	if location.SourceName == "" {
		return debugger.Breakpoint{}, invalidRequest("breakpoint source name is required")
	}

	if location.Line <= 0 {
		return debugger.Breakpoint{}, invalidRequest("breakpoint line must be positive")
	}

	if location.Column < 0 {
		return debugger.Breakpoint{}, invalidRequest("breakpoint column must not be negative")
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	d.stateMu.Lock()
	status := d.state.status
	d.stateMu.Unlock()
	if status != wiredebugger.StateCreated && status != wiredebugger.StateStopped {
		return debugger.Breakpoint{}, invalidState("breakpoints require a created or stopped debug session", nil)
	}

	if err := d.breakpoints.checkCapacity(); err != nil {
		return debugger.Breakpoint{}, err
	}

	if err := ctx.Err(); err != nil {
		return debugger.Breakpoint{}, err
	}

	value, err := panicboundary.Call(func() (debugger.Breakpoint, error) {
		return d.session.SetBreakpointAt(location, options)
	})
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("set runtime breakpoint", err); panicErr != nil {
			return debugger.Breakpoint{}, panicErr
		}

		return debugger.Breakpoint{}, invalidState("set breakpoint failed", err)
	}

	d.breakpoints.add(value)

	return value, nil
}

func (d *DebugSession) DeleteBreakpoint(ctx context.Context, breakpointID debugger.BreakpointID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if breakpointID <= 0 {
		return invalidRequest("breakpoint ID must be positive")
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	d.stateMu.Lock()
	status := d.state.status
	d.stateMu.Unlock()
	if status != wiredebugger.StateCreated && status != wiredebugger.StateStopped {
		return invalidState("breakpoints require a created or stopped debug session", nil)
	}

	value, err := d.breakpoints.get(breakpointID)
	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := panicboundary.Do(func() error { return d.session.DeleteBreakpoint(value.ID) }); err != nil {
		if panicErr := d.poisonAfterRuntimePanic("delete runtime breakpoint", err); panicErr != nil {
			return panicErr
		}

		return invalidState("delete breakpoint failed", err)
	}

	d.breakpoints.delete(breakpointID)

	return nil
}

func (d *DebugSession) Start(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, true, d.session.Start)
}

func (d *DebugSession) Continue(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.Continue)
}

func (d *DebugSession) StepOver(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.StepOver)
}

func (d *DebugSession) StepIn(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.StepIn)
}

func (d *DebugSession) StepOut(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.StepOut)
}

func (d *DebugSession) Frames(ctx context.Context) ([]debugger.Frame, error) {
	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	values, err := panicboundary.Call(d.session.Frames)
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger frames", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("frames failed", err)
	}

	return append([]debugger.Frame(nil), values...), nil
}

func (d *DebugSession) FrameLocals(ctx context.Context, frame int) ([]debugger.Variable, error) {
	if frame < 0 {
		return nil, invalidRequest("frame index must not be negative")
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	values, err := panicboundary.Call(func() ([]debugger.Variable, error) {
		return d.session.FrameLocals(frame)
	})
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger frame locals", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("frame locals failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (d *DebugSession) Variables(
	ctx context.Context,
	reference debugger.ValueReference,
) ([]debugger.Variable, error) {
	if reference <= 0 {
		return nil, invalidRequest("value reference must be positive")
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	values, err := panicboundary.Call(func() ([]debugger.Variable, error) {
		return d.session.Variables(reference)
	})
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger variables", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("variables failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (d *DebugSession) EvaluateFrame(
	ctx context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
	if frame < 0 {
		return debugger.Value{}, invalidRequest("frame index must not be negative")
	}

	if expression == "" {
		return debugger.Value{}, invalidRequest("expression is required")
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return debugger.Value{}, err
	}

	evaluateCtx, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	value, err := panicboundary.Call(func() (debugger.Value, error) {
		return d.session.EvaluateFrame(evaluateCtx, frame, expression)
	})
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("evaluate with runtime debugger", err); panicErr != nil {
			return debugger.Value{}, panicErr
		}

		return debugger.Value{}, invalidState("evaluation failed", err)
	}

	return value, nil
}

func (d *DebugSession) Watch() (DebugSubscription, error) {
	subscription, err := d.events.subscribe()
	if err != nil {
		return DebugSubscription{}, resourceExhausted("debug watcher limit reached")
	}

	return DebugSubscription{
		Current: subscription.current,
		Events:  subscription.events,
		Errors:  subscription.errors,
		Cancel:  subscription.cancel,
	}, nil
}

func (d *DebugSession) start(
	ctx context.Context,
	initial bool,
	command func(context.Context) (*debugger.Event, error),
) (wiredebugger.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return wiredebugger.Snapshot{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return wiredebugger.Snapshot{}, err
	}

	d.stateMu.Lock()
	expected := wiredebugger.StateStopped
	if initial {
		expected = wiredebugger.StateCreated
	}

	if d.state.status != expected {
		d.stateMu.Unlock()

		return wiredebugger.Snapshot{}, invalidState("debug command is not valid in the current state", nil)
	}

	d.state.beginRunning()
	kind := wiredebugger.EventContinued
	if initial {
		kind = wiredebugger.EventStarted
	}

	d.publishLocked(kind, false)
	snapshot := d.snapshotLocked()
	d.stateMu.Unlock()

	go d.runCommand(command)

	return snapshot, nil
}

func (d *DebugSession) runCommand(command func(context.Context) (*debugger.Event, error)) {
	event, err := panicboundary.Call(func() (*debugger.Event, error) {
		return command(d.ctx)
	})
	if err != nil {
		d.finishCommand(nil, err)

		return
	}

	if event == nil {
		d.finishCommand(nil, errors.New("debug execution returned no event"))

		return
	}

	d.finishCommand(event, nil)
}

func (d *DebugSession) finishCommand(event *debugger.Event, commandErr error) {
	d.operationMu.Lock()
	d.stateMu.Lock()
	if d.state.status != wiredebugger.StateRunning {
		d.stateMu.Unlock()
		d.operationMu.Unlock()

		return
	}

	terminal := false
	if commandErr != nil {
		if errors.Is(commandErr, context.Canceled) || errors.Is(context.Cause(d.ctx), context.Canceled) {
			d.state.status = wiredebugger.StateTerminated
			d.publishLocked(wiredebugger.EventTerminated, true)
		} else {
			d.state.status = wiredebugger.StateFailed
			d.state.failure = failureFromError(failure.CategoryInternalRuntime, commandErr)
			d.publishLocked(wiredebugger.EventFailed, true)
		}

		terminal = true
	} else {
		d.state.location = nil
		if event.Location != (source.Range{}) {
			location := event.Location
			d.state.location = &location
		}

		d.state.depth = event.Depth

		switch event.Reason {
		case debugger.ReasonEntry:
			d.state.status = wiredebugger.StateStopped
			d.state.reason = debugger.ReasonEntry
			d.publishLocked(wiredebugger.EventStopped, false)
		case debugger.ReasonBreakpoint:
			d.state.status = wiredebugger.StateStopped
			d.state.reason = debugger.ReasonBreakpoint
			d.state.hitIDs = append([]debugger.BreakpointID(nil), event.HitBreakpointIDs...)
			d.publishLocked(wiredebugger.EventStopped, false)
		case debugger.ReasonStep:
			d.state.status = wiredebugger.StateStopped
			d.state.reason = debugger.ReasonStep
			d.publishLocked(wiredebugger.EventStopped, false)
		case debugger.ReasonPause:
			d.state.status = wiredebugger.StateStopped
			d.state.reason = debugger.ReasonPause
			d.publishLocked(wiredebugger.EventStopped, false)
		case debugger.ReasonRuntimeError:
			d.state.status = wiredebugger.StateStopped
			d.state.reason = debugger.ReasonRuntimeError
			d.state.failure = failureFromError(failure.CategoryExecution, event.Error)
			d.publishLocked(wiredebugger.EventStopped, false)
		case debugger.ReasonCompleted:
			d.state.status = wiredebugger.StateCompleted

			if event.Output != nil {
				d.state.output = &api.Output{
					ContentType: event.Output.ContentType,
					Content:     append([]byte(nil), event.Output.Content...),
				}
			}

			d.publishLocked(wiredebugger.EventCompleted, true)
			terminal = true
		case debugger.ReasonTerminated:
			if event.Error != nil && !errors.Is(context.Cause(d.ctx), context.Canceled) {
				d.state.status = wiredebugger.StateFailed
				d.state.failure = failureFromError(failure.CategoryExecution, event.Error)
				d.publishLocked(wiredebugger.EventFailed, true)
			} else {
				d.state.status = wiredebugger.StateTerminated
				d.publishLocked(wiredebugger.EventTerminated, true)
			}

			terminal = true
		default:
			d.state.status = wiredebugger.StateFailed
			d.state.failure = failureFromError(failure.CategoryInternalRuntime, nil)
			d.publishLocked(wiredebugger.EventFailed, true)
			terminal = true
		}
	}

	d.stateMu.Unlock()
	d.operationMu.Unlock()

	if terminal {
		d.beginClose()
	}
}

func (d *DebugSession) requireStopped(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	d.stateMu.Lock()
	defer d.stateMu.Unlock()

	if d.state.status != wiredebugger.StateStopped {
		return invalidState("debug session is not stopped", nil)
	}

	return nil
}

// poisonAfterRuntimePanic applies the aggregate policy for a debugger
// implementation panic. The caller holds operationMu, so the failed transition
// is serialized with commands and breakpoint bookkeeping.
func (d *DebugSession) poisonAfterRuntimePanic(operation string, err error) error {
	var panicErr *panicboundary.Error
	if !errors.As(err, &panicErr) {
		return nil
	}

	d.stateMu.Lock()
	if !d.state.status.Terminal() {
		d.state.status = wiredebugger.StateFailed
		d.state.failure = failureFromError(failure.CategoryInternalRuntime, err)
		d.publishLocked(wiredebugger.EventFailed, true)
	}

	d.stateMu.Unlock()

	d.beginClose()

	return runtimePanicError(operation, err)
}

func (d *DebugSession) Snapshot() wiredebugger.Snapshot {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()

	return d.snapshotLocked()
}

func (d *DebugSession) snapshotLocked() wiredebugger.Snapshot {
	return d.state.snapshot()
}

func (d *DebugSession) publishLocked(kind wiredebugger.EventKind, terminal bool) {
	d.events.publish(wiredebugger.Event{
		Kind:     kind,
		Snapshot: d.snapshotLocked(),
	}, terminal)
}

// Terminal command paths commit cleanup without waiting from the command
// goroutine, allowing runtime Close implementations to wait for that command.
func (d *DebugSession) beginClose() {
	if d.close.Begin() {
		go d.settleClose()
	}
}

func (d *DebugSession) settleClose() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("debug session cleanup panicked")))
		}

		d.close.Finish(err)
	}()

	d.cancel(context.Canceled)
	err = closeAPIDebugSession(d.session)

	d.operationMu.Lock()
	d.stateMu.Lock()
	if !d.state.status.Terminal() {
		d.state.terminate()
		d.publishLocked(wiredebugger.EventTerminated, true)
	}

	d.stateMu.Unlock()
	d.operationMu.Unlock()

	d.events.close()
}
