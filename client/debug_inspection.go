package client

import (
	"context"
	"errors"
	"math"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
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

	response, err := d.client.debugClient.SetBreakpoint(ctx, &wirev1.SetBreakpointRequest{
		ConnectionId:   d.client.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
		Location: &wirev1.SourceLocation{
			File: location.File, Line: int32(location.Line), Column: int32(location.Column),
		},
	})
	if err != nil {
		return Breakpoint{}, decodeError(err)
	}

	if response.GetBreakpoint() == nil {
		return Breakpoint{}, errors.New("Wire server returned no breakpoint")
	}

	return convertBreakpoint(response.GetBreakpoint()), nil
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

	_, err := d.client.debugClient.DeleteBreakpoint(ctx, &wirev1.DeleteBreakpointRequest{
		ConnectionId:   d.client.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
		BreakpointId:   breakpointID,
	})

	return decodeError(err)
}

// Frames returns the current paused frame followed by its callers.
func (d *DebugSession) Frames(ctx context.Context) ([]Frame, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	response, err := d.client.debugClient.Frames(ctx, &wirev1.FramesRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})
	if err != nil {
		return nil, decodeError(err)
	}

	result := make([]Frame, len(response.GetFrames()))
	for i, value := range response.GetFrames() {
		result[i] = Frame{Index: int(value.GetIndex()), Name: value.GetName(), Location: convertLocation(value.GetLocation())}
	}

	return result, nil
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

	response, err := d.client.debugClient.FrameLocals(ctx, &wirev1.FrameLocalsRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id}, FrameIndex: int32(frameIndex),
	})
	if err != nil {
		return nil, decodeError(err)
	}

	result := make([]Variable, len(response.GetVariables()))
	for i, value := range response.GetVariables() {
		result[i] = convertVariable(value)
	}

	return result, nil
}

// Variables expands a non-zero debug value reference. References become stale
// after every resume.
func (d *DebugSession) Variables(ctx context.Context, reference uint64) ([]Variable, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	response, err := d.client.debugClient.Variables(ctx, &wirev1.VariablesRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id}, Reference: reference,
	})
	if err != nil {
		return nil, decodeError(err)
	}

	result := make([]Variable, len(response.GetVariables()))
	for i, value := range response.GetVariables() {
		result[i] = convertVariable(value)
	}

	return result, nil
}

// EvaluateFrame evaluates an FQL expression in one paused frame.
func (d *DebugSession) EvaluateFrame(ctx context.Context, frameIndex int, expression string) (DebugValue, error) {
	if err := d.checkOpen(); err != nil {
		return DebugValue{}, err
	}

	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return DebugValue{}, errors.New("frame index is out of range")
	}

	response, err := d.client.debugClient.EvaluateFrame(ctx, &wirev1.EvaluateFrameRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
		FrameIndex: int32(frameIndex), Expression: expression,
	})
	if err != nil {
		return DebugValue{}, decodeError(err)
	}

	if response.GetValue() == nil {
		return DebugValue{}, errors.New("Wire server returned no debug value")
	}

	return convertDebugValue(response.GetValue()), nil
}

func convertLocation(value *wirev1.SourceLocation) *Location {
	if value == nil {
		return nil
	}

	return &Location{File: value.GetFile(), Line: int(value.GetLine()), Column: int(value.GetColumn())}
}

func convertBreakpoint(value *wirev1.Breakpoint) Breakpoint {
	return Breakpoint{
		ID:              value.GetId(),
		File:            value.GetFile(),
		RequestedLine:   int(value.GetRequestedLine()),
		RequestedColumn: int(value.GetRequestedColumn()),
		Line:            int(value.GetLine()),
		Column:          int(value.GetColumn()),
		Verified:        value.GetVerified(),
	}
}

func convertDebugValue(value *wirev1.DebugValue) DebugValue {
	return DebugValue{Type: value.GetType(), Display: value.GetDisplay(), Reference: value.GetReference()}
}

func convertVariable(value *wirev1.Variable) Variable {
	return Variable{
		Name:      value.GetName(),
		Value:     convertDebugValue(value.GetValue()),
		Mutable:   value.GetMutable(),
		Parameter: value.GetParameter(),
	}
}
