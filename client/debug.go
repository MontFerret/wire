package client

import (
	"context"
	"errors"
	"fmt"
	"math"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func (c *Client) OpenDebugSession(ctx context.Context, id PlanID, parameters Parameters, options DebugSessionOptions) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	converted, err := encodeParameters(parameters)
	if err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.OpenDebugSession(ctx, &wirev1.OpenDebugSessionRequest{
		ConnectionId:      c.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: string(id)},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	if response.GetSession() == nil {
		return DebugSession{}, errors.New("Wire server returned no debug session")
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) debugCommand(id DebugSessionID) *wirev1.DebugCommand {
	return &wirev1.DebugCommand{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
	}
}

func (c *Client) StartDebug(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.StartDebug(ctx, &wirev1.StartDebugRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) Continue(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.Continue(ctx, &wirev1.ContinueRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) Pause(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.Pause(ctx, &wirev1.PauseRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) Next(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.Next(ctx, &wirev1.NextRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) StepIn(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.StepIn(ctx, &wirev1.StepInRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) StepOut(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.StepOut(ctx, &wirev1.StepOutRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) StopDebug(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}
	response, err := c.debugClient.StopDebug(ctx, &wirev1.StopDebugRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}
	return convertDebugSession(response.GetSession()), nil
}

func (c *Client) SetBreakpoints(ctx context.Context, id DebugSessionID, file string, locations []BreakpointLocation) ([]Breakpoint, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	converted := make([]*wirev1.BreakpointLocation, len(locations))
	for i, value := range locations {
		if value.Line <= 0 || value.Line > math.MaxInt32 || value.Column <= 0 || value.Column > math.MaxInt32 {
			return nil, fmt.Errorf("breakpoint %d has an invalid 1-based line or column", i)
		}
		converted[i] = &wirev1.BreakpointLocation{Line: int32(value.Line), Column: int32(value.Column)}
	}
	response, err := c.debugClient.SetBreakpoints(ctx, &wirev1.SetBreakpointsRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
		File:           file,
		Breakpoints:    converted,
	})
	if err != nil {
		return nil, decodeError(err)
	}
	result := make([]Breakpoint, len(response.GetBreakpoints()))
	for i, value := range response.GetBreakpoints() {
		result[i] = convertBreakpoint(value)
	}
	return result, nil
}

func (c *Client) StackTrace(ctx context.Context, id DebugSessionID) ([]StackFrame, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	response, err := c.debugClient.StackTrace(ctx, &wirev1.StackTraceRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
	})
	if err != nil {
		return nil, decodeError(err)
	}
	result := make([]StackFrame, len(response.GetFrames()))
	for i, value := range response.GetFrames() {
		result[i] = StackFrame{Index: int(value.GetIndex()), Name: value.GetName(), Location: convertLocation(value.GetLocation())}
	}
	return result, nil
}

func (c *Client) Scopes(ctx context.Context, id DebugSessionID, frameIndex int) ([]Scope, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return nil, errors.New("frame index is out of range")
	}
	response, err := c.debugClient.Scopes(ctx, &wirev1.ScopesRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
		FrameIndex:     int32(frameIndex),
	})
	if err != nil {
		return nil, decodeError(err)
	}
	result := make([]Scope, len(response.GetScopes()))
	for i, value := range response.GetScopes() {
		variables := make([]Variable, len(value.GetVariables()))
		for j, item := range value.GetVariables() {
			variables[j] = convertVariable(item)
		}
		kind := ScopeLocals
		if value.GetKind() == wirev1.ScopeKind_SCOPE_KIND_PARAMETERS {
			kind = ScopeParameters
		}
		result[i] = Scope{Kind: kind, Name: value.GetName(), Variables: variables}
	}
	return result, nil
}

func (c *Client) Variables(ctx context.Context, id DebugSessionID, reference uint64) ([]Variable, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	response, err := c.debugClient.Variables(ctx, &wirev1.VariablesRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
		Reference:      reference,
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

func (c *Client) Evaluate(ctx context.Context, id DebugSessionID, frameIndex int, expression string) (DebugValue, error) {
	if err := c.checkOpen(); err != nil {
		return DebugValue{}, err
	}
	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return DebugValue{}, errors.New("frame index is out of range")
	}
	response, err := c.debugClient.Evaluate(ctx, &wirev1.EvaluateRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
		FrameIndex:     int32(frameIndex),
		Expression:     expression,
	})
	if err != nil {
		return DebugValue{}, decodeError(err)
	}
	return convertDebugValue(response.GetValue()), nil
}

func (c *Client) ReleaseDebugSession(ctx context.Context, id DebugSessionID) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	_, err := c.debugClient.ReleaseDebugSession(ctx, &wirev1.ReleaseDebugSessionRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
	})
	return decodeError(err)
}

type DebugEvents struct {
	stream wirev1.DebugService_WatchDebugClient
}

func (c *Client) WatchDebug(ctx context.Context, id DebugSessionID) (*DebugEvents, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	stream, err := c.debugClient.WatchDebug(ctx, &wirev1.WatchDebugRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
	})
	if err != nil {
		return nil, decodeError(err)
	}
	return &DebugEvents{stream: stream}, nil
}

func (events *DebugEvents) Recv() (DebugEvent, error) {
	if events == nil || events.stream == nil {
		return DebugEvent{}, errors.New("debug event receiver is nil")
	}
	value, err := events.stream.Recv()
	if err != nil {
		return DebugEvent{}, decodeError(err)
	}
	if value.GetPayload() == nil {
		return DebugEvent{}, errors.New("Wire server returned an empty debug event")
	}
	return convertDebugEvent(value), nil
}
