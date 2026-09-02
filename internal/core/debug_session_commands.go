package core

import (
	"context"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

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

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.state.status != DebugRunning {
		return DebugSnapshot{}, invalidState("debug session is not running", nil)
	}

	if err := d.debugger.Pause(); err != nil {
		return DebugSnapshot{}, invalidState("pause failed", err)
	}

	return d.snapshotLocked(), nil
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

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.state.status != DebugCreated && d.state.status != DebugStopped {
		return debugger.Breakpoint{}, invalidState("breakpoints require a created or stopped debug session", nil)
	}

	return d.breakpoints.set(ctx, location, options)
}

func (d *DebugSession) DeleteBreakpoint(ctx context.Context, breakpointID debugger.BreakpointID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if breakpointID <= 0 {
		return invalidRequest("breakpoint ID must be positive")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.state.status != DebugCreated && d.state.status != DebugStopped {
		return invalidState("breakpoints require a created or stopped debug session", nil)
	}

	return d.breakpoints.delete(ctx, breakpointID)
}

func (d *DebugSession) Start(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, true, d.debugger.Start)
}

func (d *DebugSession) Continue(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.debugger.Continue)
}

func (d *DebugSession) Next(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.debugger.Next)
}

func (d *DebugSession) Step(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.debugger.Step)
}

func (d *DebugSession) Out(ctx context.Context) (DebugSnapshot, error) {
	return d.start(ctx, false, d.debugger.Out)
}
