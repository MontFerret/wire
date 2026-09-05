package client

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// DebugSession is one remote Unified API debugger session owned by its Plan.
type DebugSession struct {
	client *Client
	plan   *Plan
	id     string
	close  *closeState
}

// Start begins a newly created debug session. Watch publishes the running and
// subsequent stop or terminal snapshots.
func (d *DebugSession) Start(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Start(ctx, &wirev1.StartRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})

	return decodeError(err)
}

// Continue resumes a stopped debug session.
func (d *DebugSession) Continue(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Continue(ctx, &wirev1.ContinueRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})

	return decodeError(err)
}

// Pause requests a pause from a running debug session.
func (d *DebugSession) Pause(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Pause(ctx, &wirev1.PauseRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})

	return decodeError(err)
}

// StepOver resumes until the next statement without entering a called function.
func (d *DebugSession) StepOver(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.StepOver(ctx, &wirev1.StepOverRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})

	return decodeError(err)
}

// StepIn resumes until the next statement, entering a called function when
// applicable.
func (d *DebugSession) StepIn(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.StepIn(ctx, &wirev1.StepInRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})

	return decodeError(err)
}

// StepOut resumes until execution leaves the current frame.
func (d *DebugSession) StepOut(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.StepOut(ctx, &wirev1.StepOutRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})

	return decodeError(err)
}

// Stop terminates a non-terminal debug session without releasing its remote
// resource. Close performs the distinct release operation.
func (d *DebugSession) Stop(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.Terminate(ctx, &wirev1.TerminateRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
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

func (d *DebugSession) release(ctx context.Context) error {
	if closing, err := d.plan.ancestorCloseResult(ctx); closing {
		return err
	}

	if err := d.client.checkOpen(); err != nil {
		return err
	}

	_, err := d.client.debugClient.ReleaseDebugSession(ctx, &wirev1.ReleaseDebugSessionRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})

	return decodeError(err)
}
