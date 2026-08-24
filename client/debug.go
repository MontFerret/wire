package client

import (
	"context"

	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	// DebugSessionOptions controls encoded debug completion output.
	DebugSessionOptions struct {
		OutputContentType string
	}

	// DebugSession is one remote Ferret debugger session owned by its Plan.
	DebugSession struct {
		plan      *Plan
		transport *debugTransport
		id        string
		handle    *lifecycle.Handle
	}
)

func newDebugSession(plan *Plan, transport *debugTransport, id string) *DebugSession {
	return &DebugSession{plan: plan, transport: transport, id: id, handle: &lifecycle.Handle{}}
}

// NewDebugSession creates a Ferret debug session for a plan compiled with
// CompileOptions.Debuggable.
func (p *Plan) NewDebugSession(ctx context.Context, parameters Parameters, options DebugSessionOptions) (*DebugSession, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	id, err := p.debug.open(ctx, p.id, parameters, options)
	if err != nil {
		return nil, err
	}

	return newDebugSession(p, p.debug, id), nil
}

// Start begins a newly created debug session. Watch publishes the running and
// subsequent stop or terminal snapshots.
func (d *DebugSession) Start(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	return d.transport.start(ctx, d.id)
}

// Continue resumes a stopped debug session.
func (d *DebugSession) Continue(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	return d.transport.continueExecution(ctx, d.id)
}

// Pause requests a pause from a running debug session.
func (d *DebugSession) Pause(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	return d.transport.pause(ctx, d.id)
}

// Next resumes until the next statement without entering a called function.
func (d *DebugSession) Next(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	return d.transport.next(ctx, d.id)
}

// Step resumes until the next statement, entering a called function when
// applicable.
func (d *DebugSession) Step(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	return d.transport.step(ctx, d.id)
}

// Out resumes until execution leaves the current frame.
func (d *DebugSession) Out(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	return d.transport.out(ctx, d.id)
}

// Stop terminates a non-terminal debug session without releasing its remote
// resource. Close performs the distinct release operation.
func (d *DebugSession) Stop(ctx context.Context) error {
	if err := d.checkOpen(); err != nil {
		return err
	}

	return d.transport.stop(ctx, d.id)
}

// Close terminates and releases the remote debug session. Concurrent and
// repeated calls observe one retained release result.
func (d *DebugSession) Close(ctx context.Context) error {
	if d == nil || d.plan == nil || d.transport == nil || d.id == "" || d.handle == nil {
		return ErrClosed
	}

	return d.handle.Close(ctx, d.release)
}

func (d *DebugSession) checkOpen() error {
	if d == nil || d.plan == nil || d.transport == nil || d.id == "" || d.handle == nil || !d.handle.Open() {
		return ErrClosed
	}

	return d.plan.checkOpen()
}

func (d *DebugSession) release(ctx context.Context) error {
	if closing, err := d.plan.ancestorCloseResult(ctx); closing {
		return err
	}

	if err := d.plan.client.checkOpen(); err != nil {
		return err
	}

	return d.transport.release(ctx, d.id)
}
