package core

import (
	"context"
	"fmt"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

func (c *Connection) StopDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	return c.debugSessions.stop(ctx, id)
}

func (c *Connection) PauseDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	return c.debugSessions.pause(ctx, id)
}

func (c *Connection) SetBreakpoint(
	ctx context.Context,
	id DebugSessionID,
	location source.Location,
) (debugger.Breakpoint, error) {
	return c.SetBreakpointAt(ctx, id, location, debugger.BreakpointOptions{
		BindingMode: debugger.BreakpointBindNextExecutableInFile,
	})
}

func (c *Connection) SetBreakpointAt(
	ctx context.Context,
	id DebugSessionID,
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	return c.debugSessions.setBreakpointAt(ctx, id, location, options)
}

func (c *Connection) DeleteBreakpoint(
	ctx context.Context,
	id DebugSessionID,
	breakpointID debugger.BreakpointID,
) error {
	return c.debugSessions.deleteBreakpoint(ctx, id, breakpointID)
}

func (c *Connection) StartDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	return c.debugSessions.start(ctx, id)
}

func (c *Connection) ContinueDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	return c.debugSessions.resume(ctx, id, func(session *DebugSession) func(context.Context) (*debugger.Event, error) {
		return session.debugger.Continue
	})
}

func (c *Connection) NextDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	return c.debugSessions.resume(ctx, id, func(session *DebugSession) func(context.Context) (*debugger.Event, error) {
		return session.debugger.Next
	})
}

func (c *Connection) StepDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	return c.debugSessions.resume(ctx, id, func(session *DebugSession) func(context.Context) (*debugger.Event, error) {
		return session.debugger.Step
	})
}

func (c *Connection) OutDebug(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	return c.debugSessions.resume(ctx, id, func(session *DebugSession) func(context.Context) (*debugger.Event, error) {
		return session.debugger.Out
	})
}

func (s *debugSessionStore) stop(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	session, err := s.lookup(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.stop(ctx)
}

func (s *debugSessionStore) pause(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	session, err := s.lookup(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.pause()
}

func (s *debugSessionStore) setBreakpointAt(
	ctx context.Context,
	id DebugSessionID,
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

	session, err := s.lookup(id)
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	return session.setBreakpointAt(ctx, location, options)
}

func (s *debugSessionStore) deleteBreakpoint(
	ctx context.Context,
	id DebugSessionID,
	breakpointID debugger.BreakpointID,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if breakpointID <= 0 {
		return invalidRequest("breakpoint ID must be positive")
	}

	session, err := s.lookup(id)
	if err != nil {
		return err
	}

	return session.deleteBreakpoint(ctx, breakpointID)
}

func (s *debugSessionStore) start(ctx context.Context, id DebugSessionID) (DebugSnapshot, error) {
	session, err := s.lookup(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, true, session.debugger.Start)
}

func (s *debugSessionStore) resume(
	ctx context.Context,
	id DebugSessionID,
	command func(*DebugSession) func(context.Context) (*debugger.Event, error),
) (DebugSnapshot, error) {
	session, err := s.lookup(id)
	if err != nil {
		return DebugSnapshot{}, err
	}

	return session.start(ctx, false, command(session))
}

func (d *DebugSession) stop(ctx context.Context) (DebugSnapshot, error) {
	snapshot := d.snapshot()
	if !snapshot.State.terminal() {
		if err := d.Close(ctx); err != nil {
			return DebugSnapshot{}, err
		}

		snapshot = d.snapshot()
	}

	return snapshot, nil
}

func (d *DebugSession) pause() (DebugSnapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.state != DebugRunning {
		return DebugSnapshot{}, invalidState("debug session is not running", nil)
	}

	if err := d.debugger.Pause(); err != nil {
		return DebugSnapshot{}, invalidState("pause failed", err)
	}

	return d.snapshotLocked(), nil
}

func (d *DebugSession) setBreakpointAt(
	ctx context.Context,
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.state != DebugCreated && d.state != DebugStopped {
		return debugger.Breakpoint{}, invalidState("breakpoints require a created or stopped debug session", nil)
	}

	if len(d.breakpoints) >= d.maxBreakpoints {
		return debugger.Breakpoint{}, resourceExhausted("breakpoint limit reached")
	}

	if err := ctx.Err(); err != nil {
		return debugger.Breakpoint{}, err
	}

	value, err := d.debugger.SetBreakpointAt(location, options)
	if err != nil {
		return debugger.Breakpoint{}, invalidState("set breakpoint failed", err)
	}

	d.breakpoints[value.ID] = value

	return value, nil
}

func (d *DebugSession) deleteBreakpoint(ctx context.Context, breakpointID debugger.BreakpointID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.state != DebugCreated && d.state != DebugStopped {
		return invalidState("breakpoints require a created or stopped debug session", nil)
	}

	value, exists := d.breakpoints[breakpointID]
	if !exists {
		return notFound(ErrorBreakpointNotFound, fmt.Sprint(breakpointID))
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.debugger.DeleteBreakpoint(value.ID); err != nil {
		return invalidState("delete breakpoint failed", err)
	}

	delete(d.breakpoints, breakpointID)

	return nil
}
