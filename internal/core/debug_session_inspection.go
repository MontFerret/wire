package core

import (
	"context"

	"github.com/MontFerret/api/debugger"
)

func (d *DebugSession) Frames(ctx context.Context) ([]debugger.Frame, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.requireStoppedLocked(ctx); err != nil {
		return nil, err
	}

	values, err := d.debugger.Frames()
	if err != nil {
		return nil, invalidState("frames failed", err)
	}

	return append([]debugger.Frame(nil), values...), nil
}

func (d *DebugSession) FrameLocals(ctx context.Context, frame int) ([]debugger.Variable, error) {
	if frame < 0 {
		return nil, invalidRequest("frame index must not be negative")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.requireStoppedLocked(ctx); err != nil {
		return nil, err
	}

	values, err := d.debugger.FrameLocals(frame)
	if err != nil {
		return nil, invalidState("frame locals failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (d *DebugSession) Variables(
	ctx context.Context,
	reference debugger.ValueReference,
) ([]debugger.Variable, error) {
	if reference <= 0 {
		return nil, invalidRequest("value reference must be positive")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.requireStoppedLocked(ctx); err != nil {
		return nil, err
	}

	values, err := d.debugger.Variables(reference)
	if err != nil {
		return nil, invalidState("variables failed", err)
	}

	return append([]debugger.Variable(nil), values...), nil
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

	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.requireStoppedLocked(ctx); err != nil {
		return debugger.Value{}, err
	}

	evaluateCtx, cancel := d.operationContext(ctx)
	defer cancel()

	value, err := d.debugger.EvaluateFrame(evaluateCtx, frame, expression)
	if err != nil {
		return debugger.Value{}, invalidState("evaluation failed", err)
	}

	return value, nil
}
