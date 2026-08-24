package client

import (
	"context"
	"errors"
	"fmt"
	"math"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// OpenDebugSession creates a connection-owned Ferret debug session for a plan
// compiled with CompileOptions.Debuggable.
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

// StartDebug begins a newly created debug session and returns its running
// snapshot. WatchDebug publishes the subsequent stop or terminal event.
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

// Continue resumes a stopped debug session.
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

// Pause requests a pause from a running debug session.
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

// Next resumes until the next statement without entering a called function.
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

// Step resumes until the next statement, entering a called function when
// applicable.
func (c *Client) Step(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}

	response, err := c.debugClient.Step(ctx, &wirev1.StepRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}

	return convertDebugSession(response.GetSession()), nil
}

// Out resumes until execution leaves the current frame.
func (c *Client) Out(ctx context.Context, id DebugSessionID) (DebugSession, error) {
	if err := c.checkOpen(); err != nil {
		return DebugSession{}, err
	}

	response, err := c.debugClient.Out(ctx, &wirev1.OutRequest{Command: c.debugCommand(id)})
	if err != nil {
		return DebugSession{}, decodeError(err)
	}

	return convertDebugSession(response.GetSession()), nil
}

// StopDebug terminates a non-terminal debug session without releasing its ID.
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

// SetBreakpoint adds one Ferret breakpoint. Line must be positive; column zero
// means Ferret's unspecified column.
func (c *Client) SetBreakpoint(ctx context.Context, id DebugSessionID, location Location) (Breakpoint, error) {
	if err := c.checkOpen(); err != nil {
		return Breakpoint{}, err
	}

	if location.File == "" {
		return Breakpoint{}, errors.New("breakpoint file is required")
	}

	if location.Line <= 0 || location.Line > math.MaxInt32 || location.Column < 0 || location.Column > math.MaxInt32 {
		return Breakpoint{}, errors.New("breakpoint has an invalid line or column")
	}

	response, err := c.debugClient.SetBreakpoint(ctx, &wirev1.SetBreakpointRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
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
func (c *Client) DeleteBreakpoint(ctx context.Context, id DebugSessionID, breakpointID uint64) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	if breakpointID == 0 {
		return errors.New("breakpoint ID must be positive")
	}

	_, err := c.debugClient.DeleteBreakpoint(ctx, &wirev1.DeleteBreakpointRequest{
		ConnectionId:   c.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
		BreakpointId:   breakpointID,
	})

	return decodeError(err)
}

// Frames returns the current paused frame followed by its callers.
func (c *Client) Frames(ctx context.Context, id DebugSessionID) ([]Frame, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	response, err := c.debugClient.Frames(ctx, &wirev1.FramesRequest{
		ConnectionId: c.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
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
func (c *Client) FrameLocals(ctx context.Context, id DebugSessionID, frameIndex int) ([]Variable, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return nil, errors.New("frame index is out of range")
	}

	response, err := c.debugClient.FrameLocals(ctx, &wirev1.FrameLocalsRequest{
		ConnectionId: c.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: string(id)}, FrameIndex: int32(frameIndex),
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
func (c *Client) Variables(ctx context.Context, id DebugSessionID, reference uint64) ([]Variable, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	response, err := c.debugClient.Variables(ctx, &wirev1.VariablesRequest{
		ConnectionId: c.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: string(id)}, Reference: reference,
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
func (c *Client) EvaluateFrame(ctx context.Context, id DebugSessionID, frameIndex int, expression string) (DebugValue, error) {
	if err := c.checkOpen(); err != nil {
		return DebugValue{}, err
	}

	if frameIndex < 0 || frameIndex > math.MaxInt32 {
		return DebugValue{}, errors.New("frame index is out of range")
	}

	response, err := c.debugClient.EvaluateFrame(ctx, &wirev1.EvaluateFrameRequest{
		ConnectionId: c.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
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

// ReleaseDebugSession terminates and releases a debug session. The ID becomes
// stale when cleanup completes.
func (c *Client) ReleaseDebugSession(ctx context.Context, id DebugSessionID) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	_, err := c.debugClient.ReleaseDebugSession(ctx, &wirev1.ReleaseDebugSessionRequest{
		ConnectionId: c.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
	})

	return decodeError(err)
}

// DebugEvents receives the current debug snapshot followed by ordered state
// changes until the terminal event or stream cancellation.
type DebugEvents struct {
	stream wirev1.DebugService_WatchDebugClient
	cancel context.CancelFunc
}

// WatchDebug opens an ordered watch tied to both ctx and the Client's logical
// lifecycle.
func (c *Client) WatchDebug(ctx context.Context, id DebugSessionID) (*DebugEvents, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	watchCtx, cancel := c.watchContext(ctx)
	stream, err := c.debugClient.WatchDebug(watchCtx, &wirev1.WatchDebugRequest{
		ConnectionId: c.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: string(id)},
	})
	if err != nil {
		cancel()
		return nil, decodeError(err)
	}

	return &DebugEvents{stream: stream, cancel: cancel}, nil
}

// Recv blocks for the next ordered debug event. It releases the local stream
// when a terminal event or error is observed.
func (events *DebugEvents) Recv() (DebugEvent, error) {
	if events == nil || events.stream == nil {
		return DebugEvent{}, errors.New("debug event receiver is nil")
	}

	value, err := events.stream.Recv()
	if err != nil {
		events.cancel()
		return DebugEvent{}, decodeError(err)
	}

	if value.GetPayload() == nil {
		events.cancel()
		return DebugEvent{}, fmt.Errorf("Wire server returned an empty debug event")
	}

	event := convertDebugEvent(value)
	if event.Kind == DebugEventCompleted || event.Kind == DebugEventFailed || event.Kind == DebugEventTerminated {
		events.cancel()
	}

	return event, nil
}
