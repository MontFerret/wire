package client

import (
	"context"
	"errors"
	"math"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// SetBreakpoint adds one runtime breakpoint. Line must be positive; column zero
// means unspecified.
func (d *DebugSession) SetBreakpoint(ctx context.Context, location source.Location) (debugger.Breakpoint, error) {
	if err := d.checkOpen(); err != nil {
		return debugger.Breakpoint{}, err
	}

	if location.File == "" {
		return debugger.Breakpoint{}, errors.New("breakpoint file is required")
	}

	if location.Line <= 0 || location.Column < 0 {
		return debugger.Breakpoint{}, errors.New("breakpoint has an invalid line or column")
	}

	response, err := d.client.debugClient.SetBreakpoint(ctx, &wirev1.SetBreakpointRequest{
		ConnectionId:   d.client.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
		Location: &wirev1.Location{
			File: location.File,
			Position: &wirev1.Position{
				Line: int64(location.Line), Column: int64(location.Column),
			},
		},
	})
	if err != nil {
		return debugger.Breakpoint{}, decodeError(err)
	}

	if response.GetBreakpoint() == nil {
		return debugger.Breakpoint{}, invalidDebuggerResponse("breakpoint is missing")
	}

	return convertBreakpoint(response.GetBreakpoint())
}

// DeleteBreakpoint removes one server-issued breakpoint from a created or
// stopped session.
func (d *DebugSession) DeleteBreakpoint(ctx context.Context, breakpointID debugger.BreakpointID) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	if breakpointID <= 0 {
		return errors.New("breakpoint ID must be positive")
	}

	_, err := d.client.debugClient.DeleteBreakpoint(ctx, &wirev1.DeleteBreakpointRequest{
		ConnectionId:   d.client.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
		BreakpointId:   uint64(breakpointID),
	})

	return decodeError(err)
}

// Frames returns the current paused frame followed by its callers. The slice
// index is the frame index accepted by FrameLocals and EvaluateFrame.
func (d *DebugSession) Frames(ctx context.Context) ([]debugger.Frame, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	response, err := d.client.debugClient.Frames(ctx, &wirev1.FramesRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})
	if err != nil {
		return nil, decodeError(err)
	}

	result := make([]debugger.Frame, len(response.GetFrames()))
	for i, value := range response.GetFrames() {
		converted, err := convertFrame(value, i)
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}

	return result, nil
}

// FrameLocals returns runtime variables for a paused frame. Parameters are
// identified by debugger.Variable.Param.
func (d *DebugSession) FrameLocals(ctx context.Context, frameIndex int) ([]debugger.Variable, error) {
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

	result := make([]debugger.Variable, len(response.GetVariables()))
	for i, value := range response.GetVariables() {
		converted, err := convertVariable(value)
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}

	return result, nil
}

// Variables expands a non-zero debug value reference. References become stale
// after every resume.
func (d *DebugSession) Variables(ctx context.Context, reference debugger.ValueReference) ([]debugger.Variable, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	if reference <= 0 {
		return nil, errors.New("value reference must be positive")
	}

	response, err := d.client.debugClient.Variables(ctx, &wirev1.VariablesRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id}, Reference: uint64(reference),
	})
	if err != nil {
		return nil, decodeError(err)
	}

	result := make([]debugger.Variable, len(response.GetVariables()))
	for i, value := range response.GetVariables() {
		converted, err := convertVariable(value)
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}

	return result, nil
}

// EvaluateFrame evaluates an FQL expression in one paused frame.
func (d *DebugSession) EvaluateFrame(ctx context.Context, frameIndex int, expression string) (debugger.Value, error) {
	if err := d.checkOpen(); err != nil {
		return debugger.Value{}, err
	}

	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return debugger.Value{}, errors.New("frame index is out of range")
	}

	response, err := d.client.debugClient.EvaluateFrame(ctx, &wirev1.EvaluateFrameRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
		FrameIndex: int32(frameIndex), Expression: expression,
	})
	if err != nil {
		return debugger.Value{}, decodeError(err)
	}

	if response.GetValue() == nil {
		return debugger.Value{}, invalidDebuggerResponse("debug value is missing")
	}

	return convertDebugValue(response.GetValue())
}

func convertBreakpoint(value *wirev1.Breakpoint) (debugger.Breakpoint, error) {
	id, err := debuggerIDFromProto[debugger.BreakpointID](value.GetId(), "breakpoint ID", false)
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	requested, err := convertSourceLocation(value.GetRequestedLocation())
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	if requested == nil {
		return debugger.Breakpoint{}, invalidDebuggerResponse("requested breakpoint location is missing")
	}

	resolved, err := convertSourceRange(value.GetLocation())
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	if value.GetBound() && resolved == nil {
		return debugger.Breakpoint{}, invalidDebuggerResponse("bound breakpoint has no resolved location")
	}

	pointID, err := debuggerIDFromProto[debugger.PointID](value.GetPointId(), "breakpoint point ID", true)
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	functionID, err := debuggerIDFromProto[debugger.FunctionID](value.GetFunctionId(), "breakpoint function ID", true)
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	bindingMode, err := convertBreakpointBindingMode(value.GetBindingMode())
	if err != nil {
		return debugger.Breakpoint{}, err
	}

	var location source.Range
	if resolved != nil {
		location = *resolved
	}

	return debugger.Breakpoint{
		Location:          location,
		RequestedLocation: *requested,
		ID:                id,
		PointID:           pointID,
		FunctionID:        functionID,
		BindingMode:       bindingMode,
		Bound:             value.GetBound(),
	}, nil
}

func convertFrame(value *wirev1.Frame, index int) (debugger.Frame, error) {
	if value == nil {
		return debugger.Frame{}, invalidDebuggerResponse("frame %d is missing", index)
	}

	location, err := convertSourceLocation(value.GetLocation())
	if err != nil {
		return debugger.Frame{}, err
	}
	functionID, err := debuggerIDFromProto[debugger.FunctionID](value.GetFunctionId(), "frame function ID", true)
	if err != nil {
		return debugger.Frame{}, err
	}

	result := debugger.Frame{Name: value.GetName(), FunctionID: functionID}
	if location != nil {
		result.Location = *location
	}

	return result, nil
}

func convertBreakpointBindingMode(value wirev1.BreakpointBindingMode) (debugger.BreakpointBindingMode, error) {
	switch value {
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FILE:
		return debugger.BreakpointBindNextExecutableInFile, nil
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_EXACT:
		return debugger.BreakpointBindExact, nil
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FUNCTION:
		return debugger.BreakpointBindNextExecutableInFunction, nil
	default:
		return 0, invalidDebuggerResponse("unknown breakpoint binding mode %d", value)
	}
}

func convertDebugValue(value *wirev1.DebugValue) (debugger.Value, error) {
	if value == nil {
		return debugger.Value{}, invalidDebuggerResponse("debug value is missing")
	}

	reference, err := debuggerIDFromProto[debugger.ValueReference](value.GetReference(), "debug value reference", true)
	if err != nil {
		return debugger.Value{}, err
	}

	return debugger.Value{Type: value.GetType(), Display: value.GetDisplay(), Reference: reference}, nil
}

func convertVariable(value *wirev1.Variable) (debugger.Variable, error) {
	if value == nil {
		return debugger.Variable{}, invalidDebuggerResponse("variable is missing")
	}

	converted, err := convertDebugValue(value.GetValue())
	if err != nil {
		return debugger.Variable{}, err
	}

	return debugger.Variable{
		Name:    value.GetName(),
		Value:   converted,
		Mutable: value.GetMutable(),
		Param:   value.GetParameter(),
	}, nil
}
