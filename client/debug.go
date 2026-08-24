package client

import (
	"context"
	"errors"
	"fmt"
	"math"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	// DebugSession is one remote Ferret debugger session owned by its Plan.
	DebugSession struct {
		client *Client
		plan   *Plan
		id     string
		close  *lifecycle.Close
	}

	// DebugEvents receives published debug snapshots until the terminal event or
	// stream cancellation.
	DebugEvents struct {
		stream wirev1.DebugService_WatchDebugClient
		cancel context.CancelFunc
	}
)

// NewDebugSession creates a Ferret debug session for a plan compiled with
// CompileOptions.Debuggable.
func (p *Plan) NewDebugSession(ctx context.Context, parameters Parameters, options DebugSessionOptions) (*DebugSession, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.debugClient.OpenDebugSession(ctx, &wirev1.OpenDebugSessionRequest{
		ConnectionId:      p.client.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: p.id},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return nil, decodeError(err)
	}

	value := response.GetSession()
	if value == nil || value.GetId().GetValue() == "" {
		return nil, errors.New("Wire server returned an invalid debug session")
	}

	return &DebugSession{client: p.client, plan: p, id: value.GetId().GetValue(), close: &lifecycle.Close{}}, nil
}

func (d *DebugSession) checkOpen() error {
	if d == nil || d.client == nil || d.plan == nil || d.id == "" || d.close == nil || d.close.Started() {
		return ErrClosed
	}

	return d.plan.checkOpen()
}

func (d *DebugSession) command() *wirev1.DebugCommand {
	return &wirev1.DebugCommand{
		ConnectionId:   d.client.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	}
}

// Start begins a newly created debug session. Watch publishes the running and
// subsequent stop or terminal snapshots.
func (d *DebugSession) Start(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.StartDebug(ctx, &wirev1.StartDebugRequest{Command: d.command()})

	return decodeError(err)
}

// Continue resumes a stopped debug session.
func (d *DebugSession) Continue(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Continue(ctx, &wirev1.ContinueRequest{Command: d.command()})

	return decodeError(err)
}

// Pause requests a pause from a running debug session.
func (d *DebugSession) Pause(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Pause(ctx, &wirev1.PauseRequest{Command: d.command()})

	return decodeError(err)
}

// Next resumes until the next statement without entering a called function.
func (d *DebugSession) Next(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Next(ctx, &wirev1.NextRequest{Command: d.command()})

	return decodeError(err)
}

// Step resumes until the next statement, entering a called function when
// applicable.
func (d *DebugSession) Step(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Step(ctx, &wirev1.StepRequest{Command: d.command()})

	return decodeError(err)
}

// Out resumes until execution leaves the current frame.
func (d *DebugSession) Out(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Out(ctx, &wirev1.OutRequest{Command: d.command()})

	return decodeError(err)
}

// Stop terminates a non-terminal debug session without releasing its remote
// resource. Close performs the distinct release operation.
func (d *DebugSession) Stop(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.StopDebug(ctx, &wirev1.StopDebugRequest{Command: d.command()})

	return decodeError(err)
}

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

// Close terminates and releases the remote debug session. Concurrent and
// repeated calls observe one retained release result.
func (d *DebugSession) Close(ctx context.Context) error {
	if d == nil || d.client == nil || d.plan == nil || d.id == "" || d.close == nil {
		return ErrClosed
	}

	if d.close.Begin() {
		go settleHandleClose(ctx, "debug session", d.close, d.release)
	}

	return d.close.Wait(ctx)
}

func (d *DebugSession) release(ctx context.Context) error {
	if closing, err := d.plan.ancestorCloseResult(ctx); closing {
		return err
	}

	return d.client.releaseDebugSession(ctx, d.id)
}

func (c *Client) releaseDebugSession(ctx context.Context, id string) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	_, err := c.debugClient.ReleaseDebugSession(ctx, &wirev1.ReleaseDebugSessionRequest{
		ConnectionId: c.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: id},
	})

	return decodeError(err)
}

// Watch opens an ordered event stream tied to both ctx and the Client's
// logical lifecycle. It begins with the latest state published by the server.
func (d *DebugSession) Watch(ctx context.Context) (*DebugEvents, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	watchCtx, cancel := d.client.watchContext(ctx)
	stream, err := d.client.debugClient.WatchDebug(watchCtx, &wirev1.WatchDebugRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
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
