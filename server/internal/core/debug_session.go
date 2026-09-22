package core

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/lifecycle"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

// DebugSession owns a hosted debugger, its command state, breakpoints, and event subscriptions.
type DebugSession struct {
	// operationMu serializes state-dependent operations and command commits.
	// The active runtime resume and close paths intentionally do not hold it
	// so pause and cancellation can reach the hosted debugger.
	operationMu sync.Mutex
	// stateMu protects only Wire-visible state and never spans a runtime call.
	stateMu        sync.Mutex
	id             DebugSessionID
	plan           *Plan
	session        debugger.Session
	ctx            context.Context
	cancel         context.CancelCauseFunc
	state          debugSessionState
	breakpoints    *breakpointSet
	events         *eventStream[wiredebugger.Event]
	close          lifecycle.Close
	release        lifecycle.Close
	commands       sync.WaitGroup
	poisoned       error
	commandStreams int
	done           chan struct{}
}

func newDebugSession(plan *Plan, hosted debugger.Session) *DebugSession {
	ctx, cancel := context.WithCancelCause(plan.store.ctx)
	session := &DebugSession{
		id:          DebugSessionID(uuid.NewString()),
		plan:        plan,
		session:     hosted,
		ctx:         ctx,
		cancel:      cancel,
		done:        make(chan struct{}),
		state:       debugSessionState{status: wiredebugger.StateCreated},
		breakpoints: newBreakpointSet(plan.store.limits.Breakpoints),
		events:      newEventStream(plan.store.limits.Watchers, cloneDebugEvent, sequenceDebugEvent),
	}
	session.publishLocked(wiredebugger.EventCreated, false)

	return session
}

// Release closes the hosted debugger and removes it from its plan's resource store.
// Caller cancellation stops waiting without abandoning teardown.
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

// Close stops the hosted debugger once, retaining the logical handle until Release.
func (d *DebugSession) Close(ctx context.Context) error {
	d.beginClose()

	return d.close.Wait(ctx)
}

// ID identifies this debug session within its logical connection.
func (d *DebugSession) ID() DebugSessionID {
	return d.id
}

// Stop closes a nonterminal debugger and returns its terminal snapshot.
func (d *DebugSession) Stop(ctx context.Context) (wiredebugger.Snapshot, error) {
	if err := d.Close(ctx); err != nil {
		return d.Snapshot(), err
	}

	return d.Snapshot(), nil
}

// Pause requests interruption of a running debugger; a later event reports the stop.
func (d *DebugSession) Pause(ctx context.Context) (wiredebugger.Snapshot, error) {
	if err := debugContextError(ctx); err != nil {
		return wiredebugger.Snapshot{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := debugContextError(ctx); err != nil {
		return wiredebugger.Snapshot{}, err
	}

	d.stateMu.Lock()
	if d.state.status != wiredebugger.StateRunning {
		d.stateMu.Unlock()

		return wiredebugger.Snapshot{}, invalidState("debug session is not running", nil)
	}

	d.stateMu.Unlock()

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	if err := panicboundary.Do(func() error { return d.session.Pause(operation) }); err != nil {
		if panicErr := d.poisonAfterRuntimePanic("pause runtime debugger", err); panicErr != nil {
			return wiredebugger.Snapshot{}, panicErr
		}

		return wiredebugger.Snapshot{}, invalidState("pause failed", err)
	}

	return d.Snapshot(), nil
}

// SetBreakpoint binds to the next executable location in the requested source.
func (d *DebugSession) SetBreakpoint(
	ctx context.Context,
	location source.Location,
) (debugger.Breakpoint, error) {
	return d.setBreakpoint(ctx, location, nil)
}

// SetBreakpointAt validates and installs a breakpoint in a nonterminal debugger.
func (d *DebugSession) SetBreakpointAt(
	ctx context.Context,
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	return d.setBreakpoint(ctx, location, &options)
}

func (d *DebugSession) setBreakpoint(ctx context.Context, location source.Location, configured *debugger.BreakpointOptions) (debugger.Breakpoint, error) {
	options := debugger.BreakpointOptions{}

	if configured != nil {
		options = *configured
	}

	if err := debugContextError(ctx); err != nil {
		return debugger.Breakpoint{}, err
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

	if status.Terminal() || d.close.Started() {
		return debugger.Breakpoint{}, invalidState("debug session is terminal", nil)
	}

	if _, err := d.readBreakpoints(ctx); err != nil {
		return debugger.Breakpoint{}, err
	}

	if err := d.breakpoints.checkCapacity(); err != nil {
		return debugger.Breakpoint{}, err
	}

	if err := debugContextError(ctx); err != nil {
		return debugger.Breakpoint{}, err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	value, err := panicboundary.Call(func() (debugger.Breakpoint, error) {
		if configured == nil {
			return d.session.SetBreakpoint(operation, location)
		}

		return d.session.SetBreakpointAt(operation, location, options)
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

// DeleteBreakpoint removes a known breakpoint from a nonterminal debugger.
func (d *DebugSession) DeleteBreakpoint(ctx context.Context, breakpointID debugger.BreakpointID) error {
	if err := debugContextError(ctx); err != nil {
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

	if status.Terminal() || d.close.Started() {
		return invalidState("debug session is terminal", nil)
	}

	if _, err := d.readBreakpoints(ctx); err != nil {
		return err
	}

	value, err := d.breakpoints.get(breakpointID)
	if err != nil {
		return err
	}

	if err := debugContextError(ctx); err != nil {
		return err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	if err := panicboundary.Do(func() error { return d.session.DeleteBreakpoint(operation, value.ID) }); err != nil {
		if panicErr := d.poisonAfterRuntimePanic("delete runtime breakpoint", err); panicErr != nil {
			return panicErr
		}

		return invalidState("delete breakpoint failed", err)
	}

	d.breakpoints.delete(breakpointID)

	return nil
}

// Start begins a created debugger asynchronously and returns its running snapshot.
func (d *DebugSession) Start(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, true, d.session.Start)
}

// Continue resumes a stopped debugger asynchronously and returns its running snapshot.
func (d *DebugSession) Continue(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.Continue)
}

// StepOver resumes a stopped debugger with the hosted step-over command.
func (d *DebugSession) StepOver(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.StepOver)
}

// StepIn resumes a stopped debugger with the hosted step-in command.
func (d *DebugSession) StepIn(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.StepIn)
}

// StepOut resumes a stopped debugger with the hosted step-out command.
func (d *DebugSession) StepOut(ctx context.Context) (wiredebugger.Snapshot, error) {
	return d.start(ctx, false, d.session.StepOut)
}

// Frames returns a detached frame slice while the debugger is stopped.
func (d *DebugSession) Frames(ctx context.Context) ([]debugger.Frame, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	values, err := panicboundary.Call(func() ([]debugger.Frame, error) { return d.session.Frames(operation) })
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger frames", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("frames failed", err)
	}

	return append([]debugger.Frame(nil), values...), nil
}

// FrameLocals reads variables in a nonnegative frame index while the debugger is stopped.
func (d *DebugSession) FrameLocals(ctx context.Context, frame int) ([]debugger.Variable, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	if frame < 0 {
		return nil, invalidRequest("frame index must not be negative")
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	values, err := panicboundary.Call(func() ([]debugger.Variable, error) {
		return d.session.FrameLocals(operation, frame)
	})
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger frame locals", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("frame locals failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

// Variables expands a positive value reference while the debugger is stopped.
func (d *DebugSession) Variables(
	ctx context.Context,
	reference debugger.ValueReference,
) ([]debugger.Variable, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	if reference <= 0 {
		return nil, invalidRequest("value reference must be positive")
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	values, err := panicboundary.Call(func() ([]debugger.Variable, error) {
		return d.session.Variables(operation, reference)
	})
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime debugger variables", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("variables failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

// EvaluateFrame evaluates an expression in a stopped frame with caller and session cancellation.
func (d *DebugSession) EvaluateFrame(
	ctx context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
	if err := debugContextError(ctx); err != nil {
		return debugger.Value{}, err
	}

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

// Watch reserves a bounded subscription with the current snapshot.
// The caller must cancel the subscription to release its watcher slot.
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
	if err := debugContextError(ctx); err != nil {
		return wiredebugger.Snapshot{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := debugContextError(ctx); err != nil {
		return wiredebugger.Snapshot{}, err
	}

	d.stateMu.Lock()
	expected := wiredebugger.StateStopped

	if initial {
		expected = wiredebugger.StateCreated
	}

	if d.state.status != expected || d.close.Started() {
		d.stateMu.Unlock()

		return wiredebugger.Snapshot{}, invalidState("debug command is not valid in the current state", nil)
	}

	d.commands.Add(1)
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
	defer d.commands.Done()

	event, err := panicboundary.Call(func() (*debugger.Event, error) {
		return command(d.ctx)
	})
	if err != nil {
		d.finishCommand(event, err)

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

	var panicErr *panicboundary.Error
	if errors.As(commandErr, &panicErr) {
		d.poisoned = runtimePanicError("run runtime command", commandErr)
	}

	d.state.result = cloneCommandResult(&wiredebugger.CommandResult{Event: event, Error: commandErr})
	if event != nil {
		d.state.output = cloneOutput(event.Output)
	}

	terminal := false

	if event == nil && commandErr != nil {
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
	if err := debugContextError(ctx); err != nil {
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

	d.poisoned = runtimePanicError(operation, err)
	if !d.state.status.Terminal() {
		d.state.status = wiredebugger.StateFailed
		d.state.failure = failureFromError(failure.CategoryInternalRuntime, err)
		d.publishLocked(wiredebugger.EventFailed, true)
	}

	d.stateMu.Unlock()

	d.beginClose()

	return runtimePanicError(operation, err)
}

// Snapshot returns Wire-visible debugger state detached from mutable session storage.
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
	d.stateMu.Lock()
	started := d.close.Begin()
	d.stateMu.Unlock()

	if started {
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
		close(d.done)
	}()

	d.cancel(context.Canceled)
	err = closeAPIDebugSession(d.session)

	var panicErr *panicboundary.Error
	if errors.As(err, &panicErr) {
		d.stateMu.Lock()
		d.poisoned = err
		d.stateMu.Unlock()
	}

	d.commands.Wait()

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

// Done closes after hosted debugger cleanup and all admitted commands settle.
func (d *DebugSession) Done() <-chan struct{} { return d.done }

// RunCommand retains the caller context through the hosted command. The caller
// keeps a successful Start context alive for the execution's remaining lifetime.
func (d *DebugSession) RunCommand(ctx context.Context, initial bool, command func(context.Context) (*debugger.Event, error)) (*wiredebugger.CommandResult, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	d.operationMu.Lock()
	if err := debugContextError(ctx); err != nil {
		d.operationMu.Unlock()

		return nil, err
	}

	d.stateMu.Lock()
	expected := wiredebugger.StateStopped

	if initial {
		expected = wiredebugger.StateCreated
	}

	if d.state.status != expected || d.close.Started() {
		d.stateMu.Unlock()
		d.operationMu.Unlock()

		return nil, invalidState("debug command is not valid in the current state", nil)
	}

	previous := d.state
	d.commands.Add(1)
	d.state.beginRunning()
	kind := wiredebugger.EventContinued

	if initial {
		kind = wiredebugger.EventStarted
	}

	d.publishLocked(kind, false)
	d.stateMu.Unlock()
	d.operationMu.Unlock()
	defer d.commands.Done()
	operation, cancel := OperationContext(ctx, d.ctx)

	event, err := panicboundary.Call(func() (*debugger.Event, error) { return command(operation) })
	if initial && event != nil && event.Reason != debugger.ReasonCompleted && event.Reason != debugger.ReasonTerminated {
		context.AfterFunc(operation, cancel)
	} else {
		cancel()
	}

	if event == nil && err == nil {
		err = errors.New("debug execution returned no event")
	}

	if event == nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		d.operationMu.Lock()
		d.stateMu.Lock()
		if !d.close.Started() && d.state.status == wiredebugger.StateRunning {
			d.state = previous
			d.state.result = cloneCommandResult(&wiredebugger.CommandResult{Error: err})
			kind := wiredebugger.EventStopped

			if initial {
				kind = wiredebugger.EventCreated
			}

			d.publishLocked(kind, false)
		}

		d.stateMu.Unlock()
		d.operationMu.Unlock()
	} else {
		d.finishCommand(event, err)
	}

	return cloneCommandResult(&wiredebugger.CommandResult{Event: event, Error: err}), nil
}

// Command resolves canonical execution operations without exposing hosted state.
func (d *DebugSession) Command(name string) (func(context.Context) (*debugger.Event, error), error) {
	switch name {
	case "start":
		return d.session.Start, nil
	case "continue":
		return d.session.Continue, nil
	case "step-in":
		return d.session.StepIn, nil
	case "step-over":
		return d.session.StepOver, nil
	case "step-out":
		return d.session.StepOut, nil
	default:
		return nil, invalidRequest("invalid debug command")
	}
}

// Breakpoints reads the hosted snapshot, including after hosted Close.
func (d *DebugSession) Breakpoints(ctx context.Context) ([]debugger.Breakpoint, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	return d.readBreakpoints(ctx)
}

func (d *DebugSession) readBreakpoints(ctx context.Context) ([]debugger.Breakpoint, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	d.stateMu.Lock()
	poisoned := d.poisoned
	d.stateMu.Unlock()

	if poisoned != nil {
		return nil, poisoned
	}

	values, err := panicboundary.Call(func() ([]debugger.Breakpoint, error) { return d.session.Breakpoints(ctx) })
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("list runtime breakpoints", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("breakpoint listing failed", err)
	}

	result := append([]debugger.Breakpoint(nil), values...)
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	d.breakpoints.values = make(map[debugger.BreakpointID]debugger.Breakpoint, len(result))
	for _, value := range result {
		d.breakpoints.add(value)
	}

	return result, nil
}

// ReplaceBreakpoints preserves atomic hosted publication and charges the resulting
// source set, including unresolved requests and breakpoints in other sources.
func (d *DebugSession) ReplaceBreakpoints(ctx context.Context, sourceName string, requests []debugger.BreakpointRequest) ([]debugger.Breakpoint, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	if len(requests) > d.breakpoints.limit {
		return nil, resourceExhausted("breakpoint limit reached")
	}

	for _, request := range requests {
		if request.Position.Line <= 0 || request.Position.Column < 0 {
			return nil, invalidRequest("invalid breakpoint position")
		}

		switch request.Options.BindingMode {
		case debugger.BreakpointBindNextExecutableInSource, debugger.BreakpointBindExact, debugger.BreakpointBindNextExecutableInFunction:
		default:
			return nil, invalidRequest("invalid breakpoint binding mode")
		}
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()
	d.stateMu.Lock()
	terminal := d.state.status.Terminal() || d.close.Started()
	d.stateMu.Unlock()

	if terminal {
		return nil, invalidState("debug session is terminal", nil)
	}

	existing, err := d.readBreakpoints(ctx)
	if err != nil {
		return nil, err
	}

	canonical := sourceName
	if canonical == "" {
		canonical = d.plan.sourceName
	}

	count := len(requests)
	for _, value := range existing {
		name := value.RequestedLocation.SourceName
		if name == "" {
			name = d.plan.sourceName
		}

		if name != canonical {
			count++
		}
	}

	if count > d.breakpoints.limit {
		return nil, resourceExhausted("breakpoint limit reached")
	}

	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	values, err := panicboundary.Call(func() ([]debugger.Breakpoint, error) {
		return d.session.ReplaceBreakpoints(operation, sourceName, requests)
	})
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("replace runtime breakpoints", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("breakpoint replacement failed", err)
	}

	// Publication has succeeded. Never turn subsequent cancellation into a rollback.
	for _, value := range existing {
		name := value.RequestedLocation.SourceName
		if name == "" {
			name = d.plan.sourceName
		}

		if name == canonical {
			d.breakpoints.delete(value.ID)
		}
	}

	for _, value := range values {
		d.breakpoints.add(value)
	}

	return append([]debugger.Breakpoint(nil), values...), nil
}

// Locals invokes the hosted default-frame operation directly.
func (d *DebugSession) Locals(ctx context.Context) ([]debugger.Variable, error) {
	if err := debugContextError(ctx); err != nil {
		return nil, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return nil, err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	values, err := panicboundary.Call(func() ([]debugger.Variable, error) { return d.session.Locals(operation) })
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("read runtime locals", err); panicErr != nil {
			return nil, panicErr
		}

		return nil, invalidState("locals failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

// Evaluate invokes the hosted default-frame operation directly.
func (d *DebugSession) Evaluate(ctx context.Context, expression string) (debugger.Value, error) {
	if err := debugContextError(ctx); err != nil {
		return debugger.Value{}, err
	}

	d.operationMu.Lock()
	defer d.operationMu.Unlock()

	if err := d.requireStopped(ctx); err != nil {
		return debugger.Value{}, err
	}

	operation, cancel := OperationContext(ctx, d.ctx)
	defer cancel()

	value, err := panicboundary.Call(func() (debugger.Value, error) { return d.session.Evaluate(operation, expression) })
	if err != nil {
		if panicErr := d.poisonAfterRuntimePanic("evaluate runtime expression", err); panicErr != nil {
			return debugger.Value{}, panicErr
		}

		return debugger.Value{}, invalidState("evaluation failed", err)
	}

	return value, nil
}

// ReserveCommandStream bounds live command receivers, including Start's retained
// lifetime stream. The extra slot permits a resume with the minimum watch limit.
// Admission stays charged until the RPC handler exits, even after command completion.
func (d *DebugSession) ReserveCommandStream() (func(), error) {
	d.stateMu.Lock()
	if d.commandStreams > d.plan.store.limits.Watchers {
		d.stateMu.Unlock()

		return nil, resourceExhausted("debug command stream limit reached")
	}

	d.commandStreams++
	d.stateMu.Unlock()
	var once sync.Once

	return func() { once.Do(func() { d.stateMu.Lock(); d.commandStreams--; d.stateMu.Unlock() }) }, nil
}

// BreakpointLimit lets the transport reject oversized batches before allocating
// their decoded representation. Replacement still validates the resulting set.
func (d *DebugSession) BreakpointLimit() int { return d.breakpoints.limit }
