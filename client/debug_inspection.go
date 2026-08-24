package client

import (
	"context"
	"errors"
	"math"
)

type (
	// Location is a Ferret source position. Breakpoint column zero is unspecified.
	Location struct {
		File   string
		Line   int
		Column int
	}

	// Breakpoint describes the requested and bound Ferret breakpoint locations.
	// Its ID is passed to DeleteBreakpoint and matched against stopped events.
	Breakpoint struct {
		ID              uint64
		File            string
		RequestedLine   int
		RequestedColumn int
		Line            int
		Column          int
		Verified        bool
	}

	// DebugValue is Ferret's formatted debugger value. A non-zero Reference can
	// be passed to DebugSession.Variables until the session resumes.
	DebugValue struct {
		Type      string
		Display   string
		Reference uint64
	}

	// Variable is a Ferret debugger variable. Parameter distinguishes declared
	// query parameters from other frame locals.
	Variable struct {
		Name      string
		Value     DebugValue
		Mutable   bool
		Parameter bool
	}

	// Frame describes one paused frame and its zero-based inspection index.
	Frame struct {
		Index    int
		Name     string
		Location *Location
	}
)

// SetBreakpoint adds one Ferret breakpoint. Line must be positive; column zero
// means Ferret's unspecified column.
func (d *DebugSession) SetBreakpoint(ctx context.Context, location Location) (Breakpoint, error) {
	if err := d.checkOpen(); err != nil {
		return Breakpoint{}, err
	}

	if location.File == "" {
		return Breakpoint{}, errors.New("breakpoint file is required")
	}

	if location.Line <= 0 || location.Line > math.MaxInt32 || location.Column < 0 || location.Column > math.MaxInt32 {
		return Breakpoint{}, errors.New("breakpoint has an invalid line or column")
	}

	return d.transport.setBreakpoint(ctx, d.id, location)
}

// DeleteBreakpoint removes one server-issued breakpoint from a created or
// stopped session.
func (d *DebugSession) DeleteBreakpoint(ctx context.Context, breakpointID uint64) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	if breakpointID == 0 {
		return errors.New("breakpoint ID must be positive")
	}

	return d.transport.deleteBreakpoint(ctx, d.id, breakpointID)
}

// Frames returns the current paused frame followed by its callers.
func (d *DebugSession) Frames(ctx context.Context) ([]Frame, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	return d.transport.frames(ctx, d.id)
}

// FrameLocals returns Ferret variables for a paused frame. Parameters are
// identified by Variable.Parameter.
func (d *DebugSession) FrameLocals(ctx context.Context, frameIndex int) ([]Variable, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return nil, errors.New("frame index is out of range")
	}

	return d.transport.frameLocals(ctx, d.id, frameIndex)
}

// Variables expands a non-zero debug value reference. References become stale
// after every resume.
func (d *DebugSession) Variables(ctx context.Context, reference uint64) ([]Variable, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	return d.transport.variables(ctx, d.id, reference)
}

// EvaluateFrame evaluates an FQL expression in one paused frame.
func (d *DebugSession) EvaluateFrame(ctx context.Context, frameIndex int, expression string) (DebugValue, error) {
	if err := d.checkOpen(); err != nil {
		return DebugValue{}, err
	}

	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return DebugValue{}, errors.New("frame index is out of range")
	}

	return d.transport.evaluateFrame(ctx, d.id, frameIndex, expression)
}
