package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func (t *debugTransport) setBreakpoint(ctx context.Context, id string, location Location) (Breakpoint, error) {
	response, err := t.rpc.SetBreakpoint(ctx, &wirev1.SetBreakpointRequest{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
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

func (t *debugTransport) deleteBreakpoint(ctx context.Context, id string, breakpointID uint64) error {
	_, err := t.rpc.DeleteBreakpoint(ctx, &wirev1.DeleteBreakpointRequest{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
		BreakpointId:   breakpointID,
	})

	return decodeError(err)
}

func (t *debugTransport) frames(ctx context.Context, id string) ([]Frame, error) {
	response, err := t.rpc.Frames(ctx, &wirev1.FramesRequest{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
	})
	if err != nil {
		return nil, decodeError(err)
	}

	result := make([]Frame, len(response.GetFrames()))
	for i, value := range response.GetFrames() {
		result[i] = Frame{
			Index:    int(value.GetIndex()),
			Name:     value.GetName(),
			Location: convertLocation(value.GetLocation()),
		}
	}

	return result, nil
}

func (t *debugTransport) frameLocals(ctx context.Context, id string, frameIndex int) ([]Variable, error) {
	response, err := t.rpc.FrameLocals(ctx, &wirev1.FrameLocalsRequest{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
		FrameIndex:     int32(frameIndex),
	})
	if err != nil {
		return nil, decodeError(err)
	}

	return convertVariables(response.GetVariables()), nil
}

func (t *debugTransport) variables(ctx context.Context, id string, reference uint64) ([]Variable, error) {
	response, err := t.rpc.Variables(ctx, &wirev1.VariablesRequest{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
		Reference:      reference,
	})
	if err != nil {
		return nil, decodeError(err)
	}

	return convertVariables(response.GetVariables()), nil
}

func (t *debugTransport) evaluateFrame(
	ctx context.Context,
	id string,
	frameIndex int,
	expression string,
) (DebugValue, error) {
	response, err := t.rpc.EvaluateFrame(ctx, &wirev1.EvaluateFrameRequest{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
		FrameIndex:     int32(frameIndex),
		Expression:     expression,
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

func convertVariables(values []*wirev1.Variable) []Variable {
	result := make([]Variable, len(values))
	for i, value := range values {
		result[i] = convertVariable(value)
	}

	return result
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
