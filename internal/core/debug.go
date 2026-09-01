package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	DebugState uint8

	DebugEventKind uint8

	DebugSnapshot struct {
		ID               DebugSessionID
		PlanID           PlanID
		State            DebugState
		StopReason       debugger.Reason
		Location         source.Range
		HitBreakpointIDs []debugger.BreakpointID
		Output           *Output
		Failure          *Failure
	}

	DebugEvent struct {
		Session  DebugSessionID
		Sequence uint64
		Kind     DebugEventKind
		Snapshot DebugSnapshot
	}

	DebugSubscription struct {
		Current DebugEvent
		Events  <-chan DebugEvent
		Errors  <-chan error
		Cancel  func()
	}

	OpenDebugInput struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	DebugSession struct {
		mu             sync.Mutex
		id             DebugSessionID
		plan           *Plan
		debugger       debugger.Session
		ctx            context.Context
		cancel         context.CancelCauseFunc
		state          DebugState
		reason         debugger.Reason
		location       source.Range
		hitIDs         []debugger.BreakpointID
		output         *Output
		failure        *Failure
		breakpoints    map[debugger.BreakpointID]debugger.Breakpoint
		maxWatchers    int
		maxBreakpoints int
		sequence       uint64
		lastEvent      DebugEvent
		nextWatcher    uint64
		subscriptions  int
		watchers       map[uint64]*debugWatcher
		close          lifecycle.Close
		release        lifecycle.Close
	}

	debugWatcher struct {
		events chan DebugEvent
		errors chan error
	}
)

const (
	DebugCreated DebugState = iota + 1
	DebugRunning
	DebugStopped
	DebugCompleted
	DebugFailed
	DebugTerminated
)

const (
	DebugEventStarted DebugEventKind = iota + 1
	DebugEventContinued
	DebugEventStopped
	DebugEventCompleted
	DebugEventFailed
	DebugEventTerminated
)

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

	if d.state != expected {
		d.mu.Unlock()
		return DebugSnapshot{}, invalidState("debug command is not valid in the current state", nil)
	}

	d.state = DebugRunning
	d.reason = ""
	d.location = source.Range{}
	d.hitIDs = nil
	d.failure = nil
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
	if d.state != DebugRunning {
		d.mu.Unlock()
		return
	}

	if commandErr != nil {
		if errors.Is(commandErr, context.Canceled) || errors.Is(context.Cause(d.ctx), context.Canceled) {
			d.state = DebugTerminated
			d.publishLocked(DebugEventTerminated, true)
			d.mu.Unlock()
			d.beginClose()
			return
		}
		d.state = DebugFailed
		d.failure = failureFromError(ErrorInternal)
		d.publishLocked(DebugEventFailed, true)
		d.mu.Unlock()
		d.beginClose()
		return
	}

	d.location = event.Location
	switch event.Reason {
	case debugger.ReasonEntry:
		d.state = DebugStopped
		d.reason = debugger.ReasonEntry
		d.publishLocked(DebugEventStopped, false)
	case debugger.ReasonBreakpoint:
		d.state = DebugStopped
		d.reason = debugger.ReasonBreakpoint
		d.hitIDs = append([]debugger.BreakpointID(nil), event.HitBreakpointIDs...)
		d.publishLocked(DebugEventStopped, false)
	case debugger.ReasonStep:
		d.state = DebugStopped
		d.reason = debugger.ReasonStep
		d.publishLocked(DebugEventStopped, false)
	case debugger.ReasonPause:
		d.state = DebugStopped
		d.reason = debugger.ReasonPause
		d.publishLocked(DebugEventStopped, false)
	case debugger.ReasonRuntimeError:
		d.state = DebugStopped
		d.reason = debugger.ReasonRuntimeError
		d.failure = failureFromError(ErrorExecution)
		d.publishLocked(DebugEventStopped, false)
	case debugger.ReasonCompleted:
		d.state = DebugCompleted
		if event.Output != nil {
			d.output = &Output{ContentType: event.Output.ContentType, Content: append([]byte(nil), event.Output.Content...)}
		}
		d.publishLocked(DebugEventCompleted, true)
		terminal = true
	case debugger.ReasonTerminated:
		if event.Error != nil && !errors.Is(context.Cause(d.ctx), context.Canceled) {
			d.state = DebugFailed
			d.failure = failureFromError(ErrorExecution)
			d.publishLocked(DebugEventFailed, true)
		} else {
			d.state = DebugTerminated
			d.publishLocked(DebugEventTerminated, true)
		}
		terminal = true
	default:
		d.state = DebugFailed
		d.failure = failureFromError(ErrorInternal)
		d.publishLocked(DebugEventFailed, true)
		terminal = true
	}
	d.mu.Unlock()
	if terminal {
		d.beginClose()
	}
}

func closeAPIDebugSession(session debugger.Session) (err error) {
	defer func() {
		if recover() != nil {
			err = internalError(errors.New("runtime debug cleanup panicked"))
		}
	}()

	return session.Close()
}

func (d *DebugSession) requireStoppedLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if d.state != DebugStopped {
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
	result := DebugSnapshot{
		ID:               d.id,
		PlanID:           d.plan.id,
		State:            d.state,
		StopReason:       d.reason,
		Location:         d.location,
		HitBreakpointIDs: append([]debugger.BreakpointID(nil), d.hitIDs...),
	}

	if d.output != nil {
		result.Output = &Output{ContentType: d.output.ContentType, Content: append([]byte(nil), d.output.Content...)}
	}

	if d.failure != nil {
		result.Failure = &Failure{Category: d.failure.Category, Message: d.failure.Message, Diagnostics: cloneDiagnostics(d.failure.Diagnostics)}
	}

	return result
}

func (s DebugSnapshot) clone() DebugSnapshot {
	result := s
	result.HitBreakpointIDs = append([]debugger.BreakpointID(nil), s.HitBreakpointIDs...)
	if s.Output != nil {
		result.Output = &Output{ContentType: s.Output.ContentType, Content: append([]byte(nil), s.Output.Content...)}
	}

	if s.Failure != nil {
		result.Failure = &Failure{Category: s.Failure.Category, Message: s.Failure.Message, Diagnostics: cloneDiagnostics(s.Failure.Diagnostics)}
	}

	return result
}

func (s DebugState) terminal() bool {
	return s == DebugCompleted || s == DebugFailed || s == DebugTerminated
}

func (d *DebugSession) subscribe() (DebugSubscription, error) {
	d.mu.Lock()
	if d.subscriptions >= d.maxWatchers {
		d.mu.Unlock()
		return DebugSubscription{}, resourceExhausted("debug watcher limit reached")
	}

	d.subscriptions++
	d.nextWatcher++
	id := d.nextWatcher
	current := d.lastEvent.clone()
	if d.state.terminal() {
		events := make(chan DebugEvent)
		errorsChannel := make(chan error)
		close(events)
		close(errorsChannel)
		d.mu.Unlock()
		var once sync.Once

		return DebugSubscription{Current: current, Events: events, Errors: errorsChannel, Cancel: func() {
			once.Do(func() { d.unsubscribe(id) })
		}}, nil
	}

	watcher := &debugWatcher{events: make(chan DebugEvent, watcherBufferSize), errors: make(chan error, 1)}
	d.watchers[id] = watcher
	d.mu.Unlock()
	var once sync.Once

	return DebugSubscription{Current: current, Events: watcher.events, Errors: watcher.errors, Cancel: func() {
		once.Do(func() { d.unsubscribe(id) })
	}}, nil
}

func (d *DebugSession) publishLocked(kind DebugEventKind, terminal bool) {
	d.sequence++
	d.lastEvent = DebugEvent{Session: d.id, Sequence: d.sequence, Kind: kind, Snapshot: d.snapshotLocked()}
	for id, watcher := range d.watchers {
		select {
		case watcher.events <- d.lastEvent.clone():
			if terminal {
				d.closeWatcherLocked(id, watcher, nil)
			}
		default:
			d.closeWatcherLocked(id, watcher, ErrWatcherLagged)
		}
	}
}

func (e DebugEvent) clone() DebugEvent {
	e.Snapshot = e.Snapshot.clone()
	return e
}

func (d *DebugSession) unsubscribe(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if watcher := d.watchers[id]; watcher != nil {
		d.closeWatcherLocked(id, watcher, nil)
	}

	if d.subscriptions > 0 {
		d.subscriptions--
	}
}

func (d *DebugSession) closeWatcherLocked(id uint64, watcher *debugWatcher, err error) {
	if err != nil {
		watcher.errors <- err
	}
	close(watcher.events)
	close(watcher.errors)
	delete(d.watchers, id)
}

func (d *DebugSession) Close(ctx context.Context) error {
	d.beginClose()

	return d.close.Wait(ctx)
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
	if !d.state.terminal() {
		d.state = DebugTerminated
		d.reason = ""
		d.location = source.Range{}
		d.failure = nil
		d.publishLocked(DebugEventTerminated, true)
	}

	for id, watcher := range d.watchers {
		d.closeWatcherLocked(id, watcher, nil)
	}
	d.mu.Unlock()
}
