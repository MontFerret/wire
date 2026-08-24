package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/ferret/v2"
	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	DebugState uint8

	DebugStopReason uint8

	DebugEventKind uint8

	Location struct {
		File   string
		Line   int
		Column int
	}

	DebugValue struct {
		Type      string
		Display   string
		Reference uint64
	}

	Variable struct {
		Name      string
		Value     DebugValue
		Mutable   bool
		Parameter bool
	}

	Frame struct {
		Index    int
		Name     string
		Location Location
	}

	Breakpoint struct {
		ID              uint64
		File            string
		RequestedLine   int
		RequestedColumn int
		Line            int
		Column          int
		Verified        bool
	}

	DebugSnapshot struct {
		ID               DebugSessionID
		PlanID           PlanID
		State            DebugState
		StopReason       DebugStopReason
		Location         Location
		HitBreakpointIDs []uint64
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
		Parameters        ferretruntime.Params
		OutputContentType string
	}

	DebugSession struct {
		mu             sync.Mutex
		id             DebugSessionID
		plan           *Plan
		debugger       *ferret.DebugSession
		ctx            context.Context
		cancel         context.CancelCauseFunc
		state          DebugState
		reason         DebugStopReason
		location       Location
		hitIDs         []uint64
		output         *Output
		failure        *Failure
		breakpoints    map[uint64]ferret.DebugBreakpoint
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
	DebugStopNone DebugStopReason = iota
	DebugStopEntry
	DebugStopBreakpoint
	DebugStopStep
	DebugStopPause
	DebugStopRuntimeError
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
	command func(context.Context) (*ferret.DebugEvent, error),
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
		state := d.state
		d.mu.Unlock()
		return DebugSnapshot{}, invalidState("debug command is not valid in the current state", &ferret.DebugStateError{
			Operation: "resume",
			State:     debugStateName(state),
		})
	}

	d.state = DebugRunning
	d.reason = DebugStopNone
	d.location = Location{}
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

func (d *DebugSession) runCommand(command func(context.Context) (*ferret.DebugEvent, error)) {
	defer func() {
		if recover() != nil {
			d.finishCommand(nil, errors.New("Ferret debug execution panicked"))
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

func (d *DebugSession) finishCommand(event *ferret.DebugEvent, commandErr error) {
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
			_ = d.debugger.Close()
			return
		}
		d.state = DebugFailed
		d.failure = d.failureFrom(commandErr)
		d.publishLocked(DebugEventFailed, true)
		d.mu.Unlock()
		_ = d.debugger.Close()
		return
	}

	d.location = convertDebugLocation(event.Location)
	switch event.Reason {
	case ferret.DebugReasonEntry:
		d.state = DebugStopped
		d.reason = DebugStopEntry
		d.publishLocked(DebugEventStopped, false)
	case ferret.DebugReasonBreakpoint:
		d.state = DebugStopped
		d.reason = DebugStopBreakpoint
		d.hitIDs = make([]uint64, len(event.HitBreakpointIDs))
		for i, id := range event.HitBreakpointIDs {
			d.hitIDs[i] = uint64(id)
		}
		d.publishLocked(DebugEventStopped, false)
	case ferret.DebugReasonStep:
		d.state = DebugStopped
		d.reason = DebugStopStep
		d.publishLocked(DebugEventStopped, false)
	case ferret.DebugReasonPause:
		d.state = DebugStopped
		d.reason = DebugStopPause
		d.publishLocked(DebugEventStopped, false)
	case ferret.DebugReasonRuntimeError:
		d.state = DebugStopped
		d.reason = DebugStopRuntimeError
		d.failure = d.failureFrom(event.Error)
		d.publishLocked(DebugEventStopped, false)
	case ferret.DebugReasonCompleted:
		d.state = DebugCompleted
		d.output = convertOutput(event.Output)
		d.publishLocked(DebugEventCompleted, true)
		terminal = true
	case ferret.DebugReasonTerminated:
		if event.Error != nil && !errors.Is(context.Cause(d.ctx), context.Canceled) {
			d.state = DebugFailed
			d.failure = d.failureFrom(event.Error)
			d.publishLocked(DebugEventFailed, true)
		} else {
			d.state = DebugTerminated
			d.publishLocked(DebugEventTerminated, true)
		}
		terminal = true
	default:
		d.state = DebugFailed
		d.failure = d.failureFrom(errors.New("debug execution returned an unknown event"))
		d.publishLocked(DebugEventFailed, true)
		terminal = true
	}
	d.mu.Unlock()
	if terminal {
		_ = d.debugger.Close()
	}
}

func (d *DebugSession) failureFrom(err error) *Failure {
	return failureFromError(err, d.plan.identity)
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
		HitBreakpointIDs: append([]uint64(nil), d.hitIDs...),
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
	result.HitBreakpointIDs = append([]uint64(nil), s.HitBreakpointIDs...)
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
	if d.close.Begin() {
		go d.settleClose()
	}

	return d.close.Wait(ctx)
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
	func() {
		defer func() {
			if recover() != nil {
				err = errors.Join(err, internalError(errors.New("Ferret debug cleanup panicked")))
			}
		}()

		err = d.debugger.Close()
	}()

	d.mu.Lock()
	if !d.state.terminal() {
		d.state = DebugTerminated
		d.reason = DebugStopNone
		d.location = Location{}
		d.failure = nil
		d.publishLocked(DebugEventTerminated, true)
	}

	for id, watcher := range d.watchers {
		d.closeWatcherLocked(id, watcher, nil)
	}
	d.mu.Unlock()
}
