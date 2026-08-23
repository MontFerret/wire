package core

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/MontFerret/ferret/v2"
	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

type (
	DebugState uint8

	DebugStopReason uint8

	DebugEventKind uint8

	ScopeKind uint8

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
		Name    string
		Value   DebugValue
		Mutable bool
	}

	StackFrame struct {
		Index    int
		Name     string
		Location Location
	}

	Scope struct {
		Kind      ScopeKind
		Name      string
		Variables []Variable
	}

	BreakpointLocation struct {
		Line   int
		Column int
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
		mu          sync.Mutex
		commandMu   sync.Mutex
		id          DebugSessionID
		plan        *Plan
		debugger    *ferret.DebugSession
		ctx         context.Context
		cancel      context.CancelCauseFunc
		state       DebugState
		reason      DebugStopReason
		location    Location
		hitIDs      []uint64
		output      *Output
		failure     *Failure
		breakpoints map[string][]ferret.DebugBreakpoint
		sequence    uint64
		lastEvent   DebugEvent
		nextWatcher uint64
		watchers    map[uint64]*debugWatcher
		done        chan struct{}
		close       lifecycle.Close
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
	DebugEventOutput
	DebugEventCompleted
	DebugEventFailed
	DebugEventTerminated
)

const (
	ScopeLocals ScopeKind = iota + 1
	ScopeParameters
)

func (c *Connection) OpenDebugSession(ctx context.Context, input OpenDebugInput) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return DebugSnapshot{}, err
	}

	c.mu.Lock()
	if err := c.ensureOpenLocked(); err != nil {
		c.mu.Unlock()
		return DebugSnapshot{}, err
	}

	plan := c.plans[input.PlanID]
	if plan == nil {
		c.mu.Unlock()
		return DebugSnapshot{}, notFound(ErrorPlanNotFound, string(input.PlanID))
	}

	plan.mu.Lock()
	if plan.closing {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, notFound(ErrorPlanNotFound, string(input.PlanID))
	}

	if !plan.debuggable {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, invalidState("plan was not compiled for debugging", nil)
	}
	plan.mu.Unlock()
	c.mu.Unlock()

	options := []ferret.SessionOption{ferret.WithSessionRuntimeParams(input.Parameters)}
	if input.OutputContentType != "" {
		options = append(options, ferret.WithOutputContentType(input.OutputContentType))
	}

	debugger, err := plan.plan.NewDebugSession(ctx, options...)
	if err != nil {
		return DebugSnapshot{}, internalError(err)
	}

	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, errors.Join(err, debugger.Close())
	}

	debugCtx, cancel := context.WithCancelCause(c.ctx)
	created := &DebugSession{
		id:          DebugSessionID(uuid.NewString()),
		plan:        plan,
		debugger:    debugger,
		ctx:         debugCtx,
		cancel:      cancel,
		state:       DebugCreated,
		breakpoints: make(map[string][]ferret.DebugBreakpoint),
		watchers:    make(map[uint64]*debugWatcher),
		done:        make(chan struct{}),
	}

	c.mu.Lock()
	if err := c.ensureOpenLocked(); err != nil {
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(err, debugger.Close())
	}

	current := c.plans[input.PlanID]
	if current != plan {
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(notFound(ErrorPlanNotFound, string(input.PlanID)), debugger.Close())
	}

	plan.mu.Lock()
	if plan.closing {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(notFound(ErrorPlanNotFound, string(input.PlanID)), debugger.Close())
	}

	if err := ctx.Err(); err != nil {
		plan.mu.Unlock()
		c.mu.Unlock()
		return DebugSnapshot{}, errors.Join(err, debugger.Close())
	}

	plan.debug[created.id] = struct{}{}
	plan.mu.Unlock()
	c.debug[created.id] = created
	c.mu.Unlock()

	return created.snapshot(), nil
}

func (c *Connection) debugSession(id DebugSessionID) (*DebugSession, error) {
	if err := validateID(id, "debug session ID"); err != nil {
		return nil, err
	}

	c.mu.RLock()
	session := c.debug[id]
	c.mu.RUnlock()

	if session == nil {
		return nil, notFound(ErrorDebugSessionNotFound, string(id))
	}

	return session, nil
}

func (c *Connection) StartDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, true, session.debugger.Start)
}

func (c *Connection) ContinueDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Continue)
}

func (c *Connection) NextDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Next)
}

func (c *Connection) StepInDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Step)
}

func (c *Connection) StepOutDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, session.debugger.Out)
}

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

func debugStateName(state DebugState) string {
	switch state {
	case DebugCreated:
		return "created"
	case DebugRunning:
		return "running"
	case DebugStopped:
		return "stopped"
	case DebugCompleted:
		return "completed"
	case DebugFailed:
		return "failed"
	case DebugTerminated:
		return "terminated"
	default:
		return "unknown"
	}
}

func (d *DebugSession) runCommand(command func(context.Context) (*ferret.DebugEvent, error)) {
	defer func() {
		if recover() != nil {
			d.finishCommand(nil, errors.New("Ferret debug execution panicked"))
		}
	}()
	var event *ferret.DebugEvent
	var err error
	func() {
		d.commandMu.Lock()
		defer d.commandMu.Unlock()
		event, err = command(d.ctx)
	}()

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
	return &Failure{
		Category:    ErrorExecution,
		Message:     failureMessage(err),
		Diagnostics: diagnosticsFromError(err, d.plan.identity),
	}
}

func convertOutput(output *ferret.Output) *Output {
	if output == nil {
		return nil
	}
	return &Output{ContentType: output.ContentType, Data: append([]byte(nil), output.Content...)}
}

func convertDebugLocation(value ferret.DebugLocation) Location {
	return Location{File: value.File, Line: value.Line, Column: value.Column}
}

func (c *Connection) PauseDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}
	session.mu.Lock()
	if session.state != DebugRunning && session.state != DebugStopped {
		session.mu.Unlock()
		return DebugSnapshot{}, invalidState("debug session is not running or stopped", nil)
	}
	session.mu.Unlock()
	if err := session.debugger.Pause(); err != nil {
		return DebugSnapshot{}, invalidState("pause failed", err)
	}

	return session.snapshot(), nil
}

func (c *Connection) SetBreakpoints(
	ctx context.Context,
	id DebugSessionID,
	file string,
	locations []BreakpointLocation,
) ([]Breakpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if file == "" {
		return nil, invalidRequest("breakpoint file is required")
	}
	for _, location := range locations {
		if location.Line <= 0 || location.Column <= 0 {
			return nil, invalidRequest("breakpoint line and column must be positive")
		}
	}
	session, err := c.debugSession(id)
	if err != nil {
		return nil, err
	}
	session.commandMu.Lock()
	defer session.commandMu.Unlock()
	session.mu.Lock()
	if session.state != DebugCreated && session.state != DebugStopped {
		session.mu.Unlock()
		return nil, invalidState("breakpoints require a created or stopped debug session", nil)
	}
	existing := append([]ferret.DebugBreakpoint(nil), session.breakpoints[file]...)
	session.mu.Unlock()

	for _, breakpoint := range existing {
		if err := session.debugger.DeleteBreakpoint(breakpoint.ID); err != nil {
			return nil, invalidState("delete breakpoint failed", err)
		}
	}

	bound := make([]ferret.DebugBreakpoint, 0, len(locations))
	result := make([]Breakpoint, 0, len(locations))
	for _, location := range locations {
		breakpoint, err := session.debugger.SetBreakpointAt(
			ferret.DebugSourceLocation{File: file, Line: location.Line, Column: location.Column},
			ferret.DebugBreakpointOptions{BindingMode: ferret.DebugBreakpointBindNextExecutableInFile},
		)
		if err != nil {
			session.mu.Lock()
			session.breakpoints[file] = bound
			session.mu.Unlock()
			return nil, invalidState("set breakpoint failed", err)
		}
		bound = append(bound, breakpoint)
		result = append(result, convertBreakpoint(breakpoint))
	}
	session.mu.Lock()
	session.breakpoints[file] = bound
	session.mu.Unlock()

	return result, nil
}

func convertBreakpoint(value ferret.DebugBreakpoint) Breakpoint {
	return Breakpoint{
		ID:              uint64(value.ID),
		File:            value.File,
		RequestedLine:   value.RequestedLine,
		RequestedColumn: value.RequestedColumn,
		Line:            value.Line,
		Column:          value.Column,
		Verified:        value.Bound,
	}
}

func (c *Connection) StackTrace(ctx context.Context, id DebugSessionID) ([]StackFrame, error) {
	session, err := c.inspectableDebug(ctx, id)
	if err != nil {
		return nil, err
	}
	session.commandMu.Lock()
	defer session.commandMu.Unlock()
	if err := session.requireStopped(); err != nil {
		return nil, err
	}
	frames, err := session.debugger.Frames()
	if err != nil {
		return nil, invalidState("stack trace failed", err)
	}
	result := make([]StackFrame, len(frames))
	for i, frame := range frames {
		result[i] = StackFrame{Index: i, Name: frame.Name, Location: convertDebugLocation(frame.Location)}
	}
	return result, nil
}

func (c *Connection) Scopes(ctx context.Context, id DebugSessionID, frame int) ([]Scope, error) {
	if frame < 0 {
		return nil, invalidRequest("frame index must not be negative")
	}
	session, err := c.inspectableDebug(ctx, id)
	if err != nil {
		return nil, err
	}
	session.commandMu.Lock()
	defer session.commandMu.Unlock()
	if err := session.requireStopped(); err != nil {
		return nil, err
	}
	variables, err := session.debugger.FrameLocals(frame)
	if err != nil {
		return nil, invalidState("scopes failed", err)
	}
	locals := Scope{Kind: ScopeLocals, Name: "Locals"}
	parameters := Scope{Kind: ScopeParameters, Name: "Parameters"}
	for _, variable := range variables {
		converted := convertVariable(variable)
		if variable.Param {
			parameters.Variables = append(parameters.Variables, converted)
		} else {
			locals.Variables = append(locals.Variables, converted)
		}
	}
	return []Scope{locals, parameters}, nil
}

func (c *Connection) Variables(ctx context.Context, id DebugSessionID, reference uint64) ([]Variable, error) {
	if reference == 0 {
		return nil, invalidRequest("value reference must be positive")
	}
	session, err := c.inspectableDebug(ctx, id)
	if err != nil {
		return nil, err
	}
	session.commandMu.Lock()
	defer session.commandMu.Unlock()
	if err := session.requireStopped(); err != nil {
		return nil, err
	}
	variables, err := session.debugger.Variables(ferret.DebugValueReference(reference))
	if err != nil {
		if errors.Is(err, ferretruntime.ErrNotFound) {
			return nil, notFound(ErrorValueReferenceNotFound, fmt.Sprint(reference))
		}
		return nil, invalidState("variables failed", err)
	}
	result := make([]Variable, len(variables))
	for i, variable := range variables {
		result[i] = convertVariable(variable)
	}
	return result, nil
}

func (c *Connection) Evaluate(ctx context.Context, id DebugSessionID, frame int, expression string) (DebugValue, error) {
	if frame < 0 {
		return DebugValue{}, invalidRequest("frame index must not be negative")
	}
	if expression == "" {
		return DebugValue{}, invalidRequest("expression is required")
	}
	session, err := c.inspectableDebug(ctx, id)
	if err != nil {
		return DebugValue{}, err
	}
	session.commandMu.Lock()
	defer session.commandMu.Unlock()
	if err := session.requireStopped(); err != nil {
		return DebugValue{}, err
	}
	value, err := session.debugger.EvaluateFrame(ctx, frame, expression)
	if err != nil {
		return DebugValue{}, invalidState("evaluation failed", err)
	}
	return convertDebugValue(value), nil
}

func (c *Connection) inspectableDebug(ctx context.Context, id DebugSessionID) (*DebugSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session, err := c.debugSession(id)
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	stopped := session.state == DebugStopped
	session.mu.Unlock()
	if !stopped {
		return nil, invalidState("debug session is not stopped", nil)
	}
	return session, nil
}

func (d *DebugSession) requireStopped() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state != DebugStopped {
		return invalidState("debug session is not stopped", nil)
	}
	return nil
}

func convertVariable(value ferret.DebugVariable) Variable {
	return Variable{Name: value.Name, Value: convertDebugValue(value.Value), Mutable: value.Mutable}
}

func convertDebugValue(value ferret.DebugValue) DebugValue {
	return DebugValue{Type: value.Type, Display: value.Display, Reference: uint64(value.Reference)}
}

func (c *Connection) StopDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSnapshot{}, err
	}
	snapshot := session.snapshot()
	if !snapshot.State.terminal() {
		if err := session.Close(ctx); err != nil {
			return DebugSnapshot{}, err
		}
		snapshot = session.snapshot()
	}
	return snapshot, nil
}

func (c *Connection) WatchDebug(id DebugSessionID) (DebugSubscription, error) {
	session, err := c.debugSession(id)
	if err != nil {
		return DebugSubscription{}, err
	}
	return session.subscribe(), nil
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
		result.Output = &Output{ContentType: d.output.ContentType, Data: append([]byte(nil), d.output.Data...)}
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
		result.Output = &Output{ContentType: s.Output.ContentType, Data: append([]byte(nil), s.Output.Data...)}
	}
	if s.Failure != nil {
		result.Failure = &Failure{Category: s.Failure.Category, Message: s.Failure.Message, Diagnostics: cloneDiagnostics(s.Failure.Diagnostics)}
	}
	return result
}

func (s DebugState) terminal() bool {
	return s == DebugCompleted || s == DebugFailed || s == DebugTerminated
}

func (d *DebugSession) subscribe() DebugSubscription {
	d.mu.Lock()
	current := d.lastEvent.clone()
	if d.state.terminal() {
		events := make(chan DebugEvent)
		errorsChannel := make(chan error)
		close(events)
		close(errorsChannel)
		d.mu.Unlock()
		return DebugSubscription{Current: current, Events: events, Errors: errorsChannel, Cancel: func() {}}
	}
	d.nextWatcher++
	id := d.nextWatcher
	watcher := &debugWatcher{events: make(chan DebugEvent, watcherBufferSize), errors: make(chan error, 1)}
	d.watchers[id] = watcher
	d.mu.Unlock()
	var once sync.Once
	return DebugSubscription{Current: current, Events: watcher.events, Errors: watcher.errors, Cancel: func() {
		once.Do(func() { d.unsubscribe(id) })
	}}
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
	if terminal {
		close(d.done)
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
}

func (d *DebugSession) closeWatcherLocked(id uint64, watcher *debugWatcher, err error) {
	if err != nil {
		watcher.errors <- err
	}
	close(watcher.events)
	close(watcher.errors)
	delete(d.watchers, id)
}

func (c *Connection) ReleaseDebugSession(ctx context.Context, id DebugSessionID) error {
	if err := validateID(id, "debug session ID"); err != nil {
		return err
	}
	c.mu.Lock()
	session := c.debug[id]
	if session != nil {
		delete(c.debug, id)
		c.releasedDebug[id] = session
	} else {
		session = c.releasedDebug[id]
	}
	if session != nil {
		session.plan.mu.Lock()
		delete(session.plan.debug, id)
		session.plan.mu.Unlock()
	}
	c.mu.Unlock()
	if session == nil {
		return notFound(ErrorDebugSessionNotFound, string(id))
	}
	return session.Close(ctx)
}

func (d *DebugSession) Close(ctx context.Context) error {
	if d.close.Begin() {
		go func() {
			d.cancel(context.Canceled)
			closeErr := d.debugger.Close()
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
			d.close.Finish(closeErr)
		}()
	}
	return d.close.Wait(ctx)
}
