package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/MontFerret/wire/internal/panicboundary"
)

type (
	OpenDebugInput struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	DebugSession struct {
		// operationMu serializes state-dependent operations and command commits.
		// The active runtime resume and close paths intentionally do not hold it
		// so pause and cancellation can reach the controller.
		operationMu sync.Mutex
		// stateMu protects only Wire-visible state and never spans a runtime call.
		stateMu     sync.Mutex
		id          DebugSessionID
		owner       ConnectionID
		planID      PlanID
		controller  *DebugController
		ctx         context.Context
		cancel      context.CancelCauseFunc
		state       debugSessionState
		breakpoints *breakpointSet
		events      *eventStream[DebugEvent]
		close       lifecycle.Close
		release     lifecycle.Close
	}
)

func newDebugSession(
	id DebugSessionID,
	owner ConnectionID,
	planID PlanID,
	controller *DebugController,
	ctx context.Context,
	cancel context.CancelCauseFunc,
	maxWatchers int,
	maxBreakpoints int,
) *DebugSession {
	return &DebugSession{
		id:          id,
		owner:       owner,
		planID:      planID,
		controller:  controller,
		ctx:         ctx,
		cancel:      cancel,
		state:       debugSessionState{status: DebugCreated},
		breakpoints: newBreakpointSet(maxBreakpoints),
		events:      newEventStream[DebugEvent](maxWatchers),
	}
}

func (d *DebugSession) Close(ctx context.Context) error {
	d.beginClose()

	return d.close.Wait(ctx)
}

func (d *DebugSession) Stop(ctx context.Context) (DebugSnapshot, error) {
	snapshot := d.snapshot()
	if !snapshot.State.terminal() {
		if err := d.Close(ctx); err != nil {
			return DebugSnapshot{}, err
		}

		snapshot = d.snapshot()
	}

	return snapshot, nil
}

func (d *DebugSession) Pause(ctx context.Context) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	d.stateMu.Lock()
	if d.state.status != DebugRunning {
		d.stateMu.Unlock()

		return DebugSnapshot{}, invalidState("debug session is not running", nil)
	}
	d.stateMu.Unlock()

	if err := d.controller.Pause(); err != nil {
		if panicErr := d.poisonAfterRuntimePanic("pause runtime debugger", err); panicErr != nil {
			return DebugSnapshot{}, panicErr
		}

		return DebugSnapshot{}, invalidState("pause failed", err)
	}

	return d.snapshot(), nil
}

func (d *DebugSession) SetBreakpoint(
	ctx context.Context,
	location source.Location,
) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(ctx, location, debugger.BreakpointOptions{
		BindingMode: debugger.BreakpointBindNextExecutableInFile,
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

	if location.File == "" {
		return debugger.Breakpoint{}, invalidRequest("breakpoint file is required")
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
	if status != DebugCreated && status != DebugStopped {
		return debugger.Breakpoint{}, invalidState("breakpoints require a created or stopped debug session", nil)
	}

	if err := d.breakpoints.checkCapacity(); err != nil {
		return debugger.Breakpoint{}, err
	}

	if err := ctx.Err(); err != nil {
		return debugger.Breakpoint{}, err
	}

	value, err := d.controller.SetBreakpoint(location, options)
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
	if status != DebugCreated && status != DebugStopped {
		return invalidState("breakpoints require a created or stopped debug session", nil)
	}

	value, err := d.breakpoints.get(breakpointID)
	if err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.controller.DeleteBreakpoint(value.ID); err != nil {
		if panicErr := d.poisonAfterRuntimePanic("delete runtime breakpoint", err); panicErr != nil {
			return panicErr
		}

		return invalidState("delete breakpoint failed", err)
	}

	d.breakpoints.delete(breakpointID)

	return nil
}

func (d *DebugSession) Start(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, true, d.controller.Start)
}

func (d *DebugSession) Continue(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.controller.Continue)
}

func (d *DebugSession) Next(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.controller.Next)
}

func (d *DebugSession) Step(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.controller.Step)
}

func (d *DebugSession) Out(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.controller.Out)
}

func (d *DebugSession) Frames(ctx context.Context) ([]debugger.Frame, error) {
	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	values, err := d.controller.Frames()
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger frames", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("frames failed", err)
	}

	return values, nil
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

	values, err := d.controller.FrameLocals(frame)
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger frame locals", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("frame locals failed", err)
	}

	return values, nil
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

	values, err := d.controller.Variables(reference)
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger variables", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("variables failed", err)
	}

	return values, nil
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

	evaluateCtx, cancel := d.operationContext(ctx)
	defer cancel()

	value, err := d.controller.EvaluateFrame(evaluateCtx, frame, expression)
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
) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	d.stateMu.Lock()
	expected := DebugStopped
	if initial {
		expected = DebugCreated
	}

	if d.state.status != expected {
		d.stateMu.Unlock()

		return DebugSnapshot{}, invalidState("debug command is not valid in the current state", nil)
	}

	d.state.beginRunning()
	kind := DebugEventContinued
	if initial {
		kind = DebugEventStarted
	}

	d.publishLocked(kind, false)
	snapshot := d.snapshotLocked()
	d.stateMu.Unlock()

	go d.runCommand(command)

	return snapshot, nil
}

func (d *DebugSession) runCommand(command func(context.Context) (*debugger.Event, error)) {
	event, err := command(d.ctx)
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
	if d.state.status != DebugRunning {
		d.stateMu.Unlock()
		d.operationMu.Unlock()

		return
	}

	terminal := false
	if commandErr != nil {
		if errors.Is(commandErr, context.Canceled) || errors.Is(context.Cause(d.ctx), context.Canceled) {
			d.state.status = DebugTerminated
			d.publishLocked(DebugEventTerminated, true)
		} else {
			d.state.status = DebugFailed
			d.state.failure = failureFromError(ErrorInternal)
			d.publishLocked(DebugEventFailed, true)
		}

		terminal = true
	} else {
		d.state.location = event.Location
		d.state.depth = event.Depth

		switch event.Reason {
		case debugger.ReasonEntry:
			d.state.status = DebugStopped
			d.state.reason = debugger.ReasonEntry
			d.publishLocked(DebugEventStopped, false)
		case debugger.ReasonBreakpoint:
			d.state.status = DebugStopped
			d.state.reason = debugger.ReasonBreakpoint
			d.state.hitIDs = append([]debugger.BreakpointID(nil), event.HitBreakpointIDs...)
			d.publishLocked(DebugEventStopped, false)
		case debugger.ReasonStep:
			d.state.status = DebugStopped
			d.state.reason = debugger.ReasonStep
			d.publishLocked(DebugEventStopped, false)
		case debugger.ReasonPause:
			d.state.status = DebugStopped
			d.state.reason = debugger.ReasonPause
			d.publishLocked(DebugEventStopped, false)
		case debugger.ReasonRuntimeError:
			d.state.status = DebugStopped
			d.state.reason = debugger.ReasonRuntimeError
			d.state.failure = failureFromError(ErrorExecution)
			d.publishLocked(DebugEventStopped, false)
		case debugger.ReasonCompleted:
			d.state.status = DebugCompleted

			if event.Output != nil {
				d.state.output = &Output{
					ContentType: event.Output.ContentType,
					Content:     append([]byte(nil), event.Output.Content...),
				}
			}

			d.publishLocked(DebugEventCompleted, true)
			terminal = true
		case debugger.ReasonTerminated:
			if event.Error != nil && !errors.Is(context.Cause(d.ctx), context.Canceled) {
				d.state.status = DebugFailed
				d.state.failure = failureFromError(ErrorExecution)
				d.publishLocked(DebugEventFailed, true)
			} else {
				d.state.status = DebugTerminated
				d.publishLocked(DebugEventTerminated, true)
			}

			terminal = true
		default:
			d.state.status = DebugFailed
			d.state.failure = failureFromError(ErrorInternal)
			d.publishLocked(DebugEventFailed, true)
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

	if d.state.status != DebugStopped {
		return invalidState("debug session is not stopped", nil)
	}

	return nil
}

func (d *DebugSession) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	operation, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(d.ctx, func() {
		cancel(context.Cause(d.ctx))
	})

	return operation, func() {
		stop()
		cancel(context.Canceled)
	}
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
	if !d.state.status.terminal() {
		d.state.status = DebugFailed
		d.state.failure = failureFromError(ErrorInternal)
		d.publishLocked(DebugEventFailed, true)
	}
	d.stateMu.Unlock()

	d.beginClose()

	return runtimePanicError(operation, err)
}

func (d *DebugSession) snapshot() DebugSnapshot {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()

	return d.snapshotLocked()
}

func (d *DebugSession) snapshotLocked() DebugSnapshot {
	return d.state.snapshot(d.id, d.planID)
}

func (d *DebugSession) publishLocked(kind DebugEventKind, terminal bool) {
	d.events.publish(DebugEvent{
		Session:  d.id,
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
	err = d.controller.Close()

	d.operationMu.Lock()
	d.stateMu.Lock()
	if !d.state.status.terminal() {
		d.state.terminate()
		d.publishLocked(DebugEventTerminated, true)
	}
	d.stateMu.Unlock()
	d.operationMu.Unlock()

	d.events.close()
}
