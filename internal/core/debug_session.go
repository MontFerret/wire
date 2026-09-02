package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	OpenDebugInput struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	DebugSession struct {
		mu          sync.Mutex
		id          DebugSessionID
		owner       ConnectionID
		planID      PlanID
		debugger    debugger.Session
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
	runtime debugger.Session,
	ctx context.Context,
	cancel context.CancelCauseFunc,
	maxWatchers int,
	maxBreakpoints int,
) *DebugSession {
	return &DebugSession{
		id:          id,
		owner:       owner,
		planID:      planID,
		debugger:    runtime,
		ctx:         ctx,
		cancel:      cancel,
		state:       debugSessionState{status: DebugCreated},
		breakpoints: newBreakpointSet(runtime, maxBreakpoints),
		events:      newEventStream[DebugEvent](maxWatchers),
	}
}

func (d *DebugSession) Close(ctx context.Context) error {
	d.beginClose()

	return d.close.Wait(ctx)
}

func (d *DebugSession) start(
	ctx context.Context,
	initial bool,
	command func(context.Context) (*debugger.Event, error),
) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	d.mu.Lock()
	if err := ctx.Err(); err != nil {
		d.mu.Unlock()

		return DebugSnapshot{}, err
	}

	expected := DebugStopped
	if initial {
		expected = DebugCreated
	}

	if d.state.status != expected {
		d.mu.Unlock()

		return DebugSnapshot{}, invalidState("debug command is not valid in the current state", nil)
	}

	d.state.beginRunning()
	kind := DebugEventContinued
	if initial {
		kind = DebugEventStarted
	}

	d.publishLocked(kind, false)
	snapshot := d.snapshotLocked()
	d.mu.Unlock()

	go d.runCommand(command)

	return snapshot, nil
}

func (d *DebugSession) runCommand(command func(context.Context) (*debugger.Event, error)) {
	defer func() {
		if recover() != nil {
			d.finishCommand(nil, errors.New("runtime debug execution panicked"))
		}
	}()

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
	terminal := false
	d.mu.Lock()
	if d.state.status != DebugRunning {
		d.mu.Unlock()

		return
	}

	if commandErr != nil {
		if errors.Is(commandErr, context.Canceled) || errors.Is(context.Cause(d.ctx), context.Canceled) {
			d.state.status = DebugTerminated
			d.publishLocked(DebugEventTerminated, true)
			d.mu.Unlock()
			d.beginClose()

			return
		}

		d.state.status = DebugFailed
		d.state.failure = failureFromError(ErrorInternal)
		d.publishLocked(DebugEventFailed, true)
		d.mu.Unlock()
		d.beginClose()

		return
	}

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
	d.mu.Unlock()

	if terminal {
		d.beginClose()
	}
}

func (d *DebugSession) requireStoppedLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

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

func (d *DebugSession) snapshot() DebugSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.snapshotLocked()
}

func (d *DebugSession) snapshotLocked() DebugSnapshot {
	return d.state.snapshot(d.id, d.planID)
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
	err = closeAPIDebugSession(d.debugger)

	d.mu.Lock()
	if !d.state.status.terminal() {
		d.state.terminate()
		d.publishLocked(DebugEventTerminated, true)
	}
	d.mu.Unlock()

	d.events.close()
}
