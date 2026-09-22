package client

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

type remoteDebugSession struct {
	session          *debugSessionHandle
	ctx              context.Context
	cancel           context.CancelFunc
	commandMu        sync.Mutex
	snapshotMu       sync.Mutex
	finalBreakpoints []debugger.Breakpoint
	breakpointErr    error
	lifetimeDone     chan struct{}
	close            closeState
}

var _ debugger.Session = (*remoteDebugSession)(nil)

func newRemoteDebugSession(session *debugSessionHandle) *remoteDebugSession {
	ctx, cancel := context.WithCancel(context.Background())

	return &remoteDebugSession{session: session, ctx: ctx, cancel: cancel}
}

func (d *remoteDebugSession) Start(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, wirev1.DebugCommand_DEBUG_COMMAND_START)
}

func (d *remoteDebugSession) Continue(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, wirev1.DebugCommand_DEBUG_COMMAND_CONTINUE)
}

func (d *remoteDebugSession) StepIn(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, wirev1.DebugCommand_DEBUG_COMMAND_STEP_IN)
}

func (d *remoteDebugSession) StepOver(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, wirev1.DebugCommand_DEBUG_COMMAND_STEP_OVER)
}

func (d *remoteDebugSession) StepOut(ctx context.Context) (*debugger.Event, error) {
	return d.runCommand(ctx, wirev1.DebugCommand_DEBUG_COMMAND_STEP_OUT)
}

func (d *remoteDebugSession) runCommand(ctx context.Context, command wirev1.DebugCommand) (*debugger.Event, error) {
	if err := d.operationError(ctx); err != nil {
		return nil, err
	}

	d.commandMu.Lock()
	defer d.commandMu.Unlock()

	if err := d.operationError(ctx); err != nil {
		return nil, err
	}

	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(d.ctx, cancel)
	retained := false
	defer func() {
		if !retained {
			stop()
			cancel()
		}
	}()

	stream, err := d.session.client.debugClient.RunCommand(operation, &wirev1.RunCommandRequest{
		ConnectionId: d.session.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.session.id}, Command: command,
	})
	if err != nil {
		return nil, d.commandError(ctx, err)
	}

	response, err := stream.Recv()
	if err != nil {
		return nil, d.commandError(ctx, err)
	}

	result, err := convertCommandResult(response.GetResult())
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, invalidDebuggerResponse("command result is missing")
	}

	if command == wirev1.DebugCommand_DEBUG_COMMAND_START && result.Event != nil &&
		result.Event.Reason != debugger.ReasonCompleted && result.Event.Reason != debugger.ReasonTerminated {
		retained = true
		done := make(chan struct{})
		d.snapshotMu.Lock()
		d.lifetimeDone = done
		d.snapshotMu.Unlock()
		// Start's context remains attached after its initial stop. The server sends
		// exactly one result; closing this stream ends the hosted execution context.
		go func() {
			defer close(done)
			defer stop()
			defer cancel()
			_, _ = stream.Recv()
		}()
	}

	return result.Event, result.Error
}

func (d *remoteDebugSession) commandError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	if d.ctx.Err() != nil {
		return ErrClosed
	}

	return decodeError(err)
}

func (d *remoteDebugSession) operationError(ctx context.Context) error {
	if err := runtimeContextError(ctx); err != nil {
		return err
	}

	if d == nil || d.session == nil || d.close.Started() {
		return ErrClosed
	}

	return d.session.checkOpen()
}

func (d *remoteDebugSession) Pause(ctx context.Context) error {
	if err := d.operationError(ctx); err != nil {
		return err
	}

	return d.session.Pause(ctx)
}

func (d *remoteDebugSession) SetBreakpoint(ctx context.Context, location source.Location) (debugger.Breakpoint, error) {
	if err := d.operationError(ctx); err != nil {
		return debugger.Breakpoint{}, err
	}

	return d.session.setBreakpoint(ctx, location, nil)
}

func (d *remoteDebugSession) SetBreakpointAt(ctx context.Context, location source.Location, options debugger.BreakpointOptions) (debugger.Breakpoint, error) {
	if err := d.operationError(ctx); err != nil {
		return debugger.Breakpoint{}, err
	}

	return d.session.SetBreakpointAt(ctx, location, options)
}

func (d *remoteDebugSession) ReplaceBreakpoints(ctx context.Context, sourceName string, requests []debugger.BreakpointRequest) ([]debugger.Breakpoint, error) {
	if err := d.operationError(ctx); err != nil {
		return nil, err
	}

	return d.session.replaceBreakpoints(ctx, sourceName, requests)
}

func (d *remoteDebugSession) DeleteBreakpoint(ctx context.Context, id debugger.BreakpointID) error {
	if err := d.operationError(ctx); err != nil {
		return err
	}

	return d.session.DeleteBreakpoint(ctx, id)
}

func (d *remoteDebugSession) Breakpoints(ctx context.Context) ([]debugger.Breakpoint, error) {
	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	if d == nil || d.session == nil {
		return nil, ErrClosed
	}

	if d.close.Started() {
		// Enumeration has its own retained result, independent from cleanup errors.
		_ = d.close.Wait(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		d.snapshotMu.Lock()
		defer d.snapshotMu.Unlock()

		return append([]debugger.Breakpoint(nil), d.finalBreakpoints...), d.breakpointErr
	}

	return d.session.breakpoints(ctx)
}

func (d *remoteDebugSession) Frames(ctx context.Context) ([]debugger.Frame, error) {
	if err := d.operationError(ctx); err != nil {
		return nil, err
	}

	return d.session.Frames(ctx)
}

func (d *remoteDebugSession) Locals(ctx context.Context) ([]debugger.Variable, error) {
	if err := d.operationError(ctx); err != nil {
		return nil, err
	}

	return d.session.locals(ctx)
}

func (d *remoteDebugSession) FrameLocals(ctx context.Context, frame int) ([]debugger.Variable, error) {
	if err := d.operationError(ctx); err != nil {
		return nil, err
	}

	return d.session.FrameLocals(ctx, frame)
}

func (d *remoteDebugSession) Variables(ctx context.Context, reference debugger.ValueReference) ([]debugger.Variable, error) {
	if err := d.operationError(ctx); err != nil {
		return nil, err
	}

	return d.session.Variables(ctx, reference)
}

func (d *remoteDebugSession) Evaluate(ctx context.Context, expression string) (debugger.Value, error) {
	if err := d.operationError(ctx); err != nil {
		return debugger.Value{}, err
	}

	return d.session.evaluate(ctx, expression)
}

func (d *remoteDebugSession) EvaluateFrame(ctx context.Context, frame int, expression string) (debugger.Value, error) {
	if err := d.operationError(ctx); err != nil {
		return debugger.Value{}, err
	}

	return d.session.EvaluateFrame(ctx, frame, expression)
}

func (d *remoteDebugSession) Close() error {
	if d == nil || d.session == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, d.closeWithContext)
}

func (d *remoteDebugSession) closeWithContext(ctx context.Context) error {
	if d.close.Begin() {
		d.cancel()
		go settleHandleClose(ctx, "debugger API", &d.close, d.settleClose)
	}

	return d.close.Wait(ctx)
}

func (d *remoteDebugSession) settleClose(ctx context.Context) error {
	if closing, err := d.session.plan.ancestorCloseResult(ctx); closing {
		d.snapshotMu.Lock()
		d.breakpointErr = ErrClosed
		d.snapshotMu.Unlock()

		return errors.Join(err, d.session.Close(ctx))
	}

	_, terminateErr := d.session.client.debugClient.Terminate(ctx, &wirev1.TerminateRequest{
		ConnectionId: d.session.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.session.id},
	})
	points, pointsErr := d.session.breakpoints(ctx)
	d.snapshotMu.Lock()
	d.finalBreakpoints, d.breakpointErr = points, pointsErr
	d.snapshotMu.Unlock()
	releaseErr := d.session.Close(ctx)
	// Cancellation unblocks command receivers before Close joins their local work.
	d.commandMu.Lock()
	// All admitted receivers have returned after lifetime cancellation.
	d.snapshotMu.Lock()
	done := d.lifetimeDone
	d.snapshotMu.Unlock()
	d.commandMu.Unlock()

	if done != nil {
		<-done
	}

	return errors.Join(decodeError(terminateErr), releaseErr)
}
