package client

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

type runtimeDebugSession struct {
	session *DebugSession
	ctx     context.Context
	cancel  context.CancelFunc

	commandMu    sync.Mutex
	breakpointMu sync.Mutex
	breakpoints  map[debugger.BreakpointID]debugger.Breakpoint
}

func newRuntimeDebugSession(session *DebugSession) *runtimeDebugSession {
	ctx, cancel := context.WithCancel(context.Background())

	return &runtimeDebugSession{
		session:     session,
		ctx:         ctx,
		cancel:      cancel,
		breakpoints: make(map[debugger.BreakpointID]debugger.Breakpoint),
	}
}

func (d *runtimeDebugSession) Start(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.Start)
}

func (d *runtimeDebugSession) Continue(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.Continue)
}

func (d *runtimeDebugSession) StepIn(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.StepIn)
}

func (d *runtimeDebugSession) StepOver(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.StepOver)
}

func (d *runtimeDebugSession) StepOut(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.StepOut)
}

func (d *runtimeDebugSession) runCommand(
	ctx context.Context,
	command func(context.Context) error,
) (*debugger.Event, error) {
	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	if ctx == nil {
		ctx = context.Background()
	}

	d.commandMu.Lock()
	defer d.commandMu.Unlock()

	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(d.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	events, err := d.session.Watch(operation)
	if err != nil {
		return nil, d.commandError(ctx, err)
	}
	defer events.cancel()

	if _, err := events.Recv(); err != nil {
		return nil, d.commandError(ctx, err)
	}

	if err := command(operation); err != nil {
		return nil, d.commandError(ctx, err)
	}

	for {
		event, err := events.Recv()
		if err != nil {
			return nil, d.commandError(ctx, err)
		}

		converted, terminal, err := runtimeDebuggerEvent(event)
		if err != nil {
			return nil, err
		}

		if terminal {
			return converted, nil
		}
	}
}

func (d *runtimeDebugSession) commandError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := d.Close()

		return errors.Join(ctxErr, closeErr)
	}

	if d.ctx.Err() != nil {
		return ErrClosed
	}

	return err
}

func runtimeDebuggerEvent(event wiredebugger.Event) (*debugger.Event, bool, error) {
	snapshot := event.Snapshot
	switch snapshot.State {
	case wiredebugger.StateCreated, wiredebugger.StateRunning:
		return nil, false, nil
	case wiredebugger.StateFailed:
		if snapshot.Failure == nil {
			return nil, false, errors.New("Wire server returned a failed debug session without failure details")
		}

		return nil, false, snapshot.Failure
	case wiredebugger.StateStopped:
		result := &debugger.Event{
			Error:            snapshot.Failure,
			Reason:           snapshot.StopReason,
			HitBreakpointIDs: append([]debugger.BreakpointID(nil), snapshot.HitBreakpointIDs...),
			Depth:            snapshot.Depth,
		}
		if snapshot.Location != nil {
			result.Location = *snapshot.Location
		}

		return result, true, nil
	case wiredebugger.StateCompleted:
		result := &debugger.Event{Reason: debugger.ReasonCompleted}
		if snapshot.Output != nil {
			result.Output = &api.Output{
				ContentType: snapshot.Output.ContentType,
				Content:     append([]byte(nil), snapshot.Output.Content...),
			}
		}

		return result, true, nil
	case wiredebugger.StateTerminated:
		return &debugger.Event{Reason: debugger.ReasonTerminated}, true, nil
	default:
		return nil, false, errors.New("Wire server returned an invalid debug session state")
	}
}

func (d *runtimeDebugSession) Pause() error {
	if d == nil || d.session == nil {
		return ErrClosed
	}

	return d.session.Pause(d.ctx)
}

func (d *runtimeDebugSession) SetBreakpoint(location source.Location) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(location, debugger.BreakpointOptions{
		BindingMode: debugger.BreakpointBindNextExecutableInSource,
	})
}

func (d *runtimeDebugSession) SetBreakpointAt(
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	if d == nil || d.session == nil {
		return debugger.Breakpoint{}, ErrClosed
	}

	breakpoint, err := d.session.SetBreakpointAt(d.ctx, location, options)
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	d.breakpointMu.Lock()
	d.breakpoints[breakpoint.ID] = breakpoint
	d.breakpointMu.Unlock()

	return breakpoint, nil
}

func (d *runtimeDebugSession) DeleteBreakpoint(id debugger.BreakpointID) error {
	if d == nil || d.session == nil {
		return ErrClosed
	}

	if err := d.session.DeleteBreakpoint(d.ctx, id); err != nil {
		return err
	}

	d.breakpointMu.Lock()
	delete(d.breakpoints, id)
	d.breakpointMu.Unlock()

	return nil
}

func (d *runtimeDebugSession) Breakpoints() []debugger.Breakpoint {
	if d == nil {
		return nil
	}

	d.breakpointMu.Lock()
	result := make([]debugger.Breakpoint, 0, len(d.breakpoints))
	for _, breakpoint := range d.breakpoints {
		result = append(result, breakpoint)
	}
	d.breakpointMu.Unlock()

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result
}

func (d *runtimeDebugSession) Frames() ([]debugger.Frame, error) {
	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	return d.session.Frames(d.ctx)
}

func (d *runtimeDebugSession) Locals() ([]debugger.Variable, error) {
	return d.FrameLocals(0)
}

func (d *runtimeDebugSession) FrameLocals(frame int) ([]debugger.Variable, error) {
	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	return d.session.FrameLocals(d.ctx, frame)
}

func (d *runtimeDebugSession) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	return d.session.Variables(d.ctx, reference)
}

func (d *runtimeDebugSession) Evaluate(ctx context.Context, expression string) (debugger.Value, error) {
	return d.EvaluateFrame(ctx, 0, expression)
}

func (d *runtimeDebugSession) EvaluateFrame(
	ctx context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
	if d == nil || d.session == nil {
		return debugger.Value{}, ErrClosed
	}

	return d.session.EvaluateFrame(ctx, frame, expression)
}

func (d *runtimeDebugSession) Close() error {
	if d == nil || d.session == nil {
		return ErrClosed
	}

	d.cancel()

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, d.session.Close)
}
