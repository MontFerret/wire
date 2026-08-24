package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	// DebugSessionOptions controls encoded debug completion output.
	DebugSessionOptions struct {
		OutputContentType string
	}

	// DebugSession is one remote Ferret debugger session owned by its Plan.
	DebugSession struct {
		client *Client
		plan   *Plan
		id     string
		close  *lifecycle.Close
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
