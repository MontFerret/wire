package core

import (
	"context"

	"github.com/MontFerret/api/debugger"
)

func (c *Connection) Frames(ctx context.Context, id DebugSessionID) ([]debugger.Frame, error) {
	return c.debugSessions.frames(ctx, id)
}

func (c *Connection) FrameLocals(
	ctx context.Context,
	id DebugSessionID,
	frame int,
) ([]debugger.Variable, error) {
	return c.debugSessions.frameLocals(ctx, id, frame)
}

func (c *Connection) Variables(
	ctx context.Context,
	id DebugSessionID,
	reference debugger.ValueReference,
) ([]debugger.Variable, error) {
	return c.debugSessions.variables(ctx, id, reference)
}

func (c *Connection) EvaluateFrame(
	ctx context.Context,
	id DebugSessionID,
	frame int,
	expression string,
) (debugger.Value, error) {
	return c.debugSessions.evaluateFrame(ctx, id, frame, expression)
}

func (s *debugSessionStore) frames(ctx context.Context, id DebugSessionID) ([]debugger.Frame, error) {
	session, err := s.lookup(id)
	if err != nil {
		return nil, err
	}

	return session.frames(ctx)
}

func (s *debugSessionStore) frameLocals(
	ctx context.Context,
	id DebugSessionID,
	frame int,
) ([]debugger.Variable, error) {
	if frame < 0 {
		return nil, invalidRequest("frame index must not be negative")
	}

	session, err := s.lookup(id)
	if err != nil {
		return nil, err
	}

	return session.frameLocals(ctx, frame)
}

func (s *debugSessionStore) variables(
	ctx context.Context,
	id DebugSessionID,
	reference debugger.ValueReference,
) ([]debugger.Variable, error) {
	if reference <= 0 {
		return nil, invalidRequest("value reference must be positive")
	}

	session, err := s.lookup(id)
	if err != nil {
		return nil, err
	}

	return session.variables(ctx, reference)
}

func (s *debugSessionStore) evaluateFrame(
	ctx context.Context,
	id DebugSessionID,
	frame int,
	expression string,
) (debugger.Value, error) {
	if frame < 0 {
		return debugger.Value{}, invalidRequest("frame index must not be negative")
	}

	if expression == "" {
		return debugger.Value{}, invalidRequest("expression is required")
	}

	session, err := s.lookup(id)
	if err != nil {
		return debugger.Value{}, err
	}

	return session.evaluateFrame(ctx, frame, expression)
}

func (d *DebugSession) frames(ctx context.Context) ([]debugger.Frame, error) {
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

func (d *DebugSession) frameLocals(ctx context.Context, frame int) ([]debugger.Variable, error) {
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

func (d *DebugSession) variables(ctx context.Context, reference debugger.ValueReference) ([]debugger.Variable, error) {
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

func (d *DebugSession) evaluateFrame(
	ctx context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
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
