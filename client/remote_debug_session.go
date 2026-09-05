package client

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type remoteDebugSession struct {
	session *DebugSession
	ctx     context.Context
	cancel  context.CancelFunc

	commandMu    sync.Mutex
	breakpointMu sync.Mutex
	breakpoints  map[debugger.BreakpointID]debugger.Breakpoint
}

var _ debugger.Session = (*remoteDebugSession)(nil)

func newRemoteDebugSession(session *DebugSession) *remoteDebugSession {
	ctx, cancel := context.WithCancel(context.Background())

	return &remoteDebugSession{
		session:     session,
		ctx:         ctx,
		cancel:      cancel,
		breakpoints: make(map[debugger.BreakpointID]debugger.Breakpoint),
	}
}

func (d *remoteDebugSession) Start(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.Start)
}

func (d *remoteDebugSession) Continue(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.Continue)
}

func (d *remoteDebugSession) StepIn(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.StepIn)
}

func (d *remoteDebugSession) StepOver(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.StepOver)
}

func (d *remoteDebugSession) StepOut(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, d.session.StepOut)
}

func (d *remoteDebugSession) runCommand(
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

		converted, terminal, err := remoteDebuggerEvent(event)
		if err != nil {
			return nil, err
		}

		if terminal {
			return converted, nil
		}
	}
}

func (d *remoteDebugSession) commandError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := d.Close()

		return errors.Join(ctxErr, closeErr)
	}

	if d.ctx.Err() != nil {
		return ErrClosed
	}

	return err
}

func (d *remoteDebugSession) Pause() error {
	if d == nil || d.session == nil {
		return ErrClosed
	}

	return d.session.Pause(d.ctx)
}

func (d *remoteDebugSession) SetBreakpoint(location source.Location) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(location, debugger.BreakpointOptions{
		BindingMode: debugger.BreakpointBindNextExecutableInSource,
	})
}

func (d *remoteDebugSession) SetBreakpointAt(
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

func (d *remoteDebugSession) DeleteBreakpoint(id debugger.BreakpointID) error {
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

func (d *remoteDebugSession) Breakpoints() []debugger.Breakpoint {
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

func (d *remoteDebugSession) Frames() ([]debugger.Frame, error) {
	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	return d.session.Frames(d.ctx)
}

func (d *remoteDebugSession) Locals() ([]debugger.Variable, error) {
	return d.FrameLocals(0)
}

func (d *remoteDebugSession) FrameLocals(frame int) ([]debugger.Variable, error) {
	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	return d.session.FrameLocals(d.ctx, frame)
}

func (d *remoteDebugSession) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	return d.session.Variables(d.ctx, reference)
}

func (d *remoteDebugSession) Evaluate(ctx context.Context, expression string) (debugger.Value, error) {
	return d.EvaluateFrame(ctx, 0, expression)
}

func (d *remoteDebugSession) EvaluateFrame(
	ctx context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
	if d == nil || d.session == nil {
		return debugger.Value{}, ErrClosed
	}

	return d.session.EvaluateFrame(ctx, frame, expression)
}

func (d *remoteDebugSession) Close() error {
	if d == nil || d.session == nil {
		return ErrClosed
	}

	d.cancel()

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, d.session.Close)
}
