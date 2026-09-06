package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/execution"
)

// executionHandle is one asynchronous operation owned by a logical connection
// or durable session. Plan ownership is reached through the session.
type executionHandle struct {
	client  *connectionHandle
	session *sessionHandle
	id      string
	close   *closeState
}

func newExecutionHandle(
	client *connectionHandle,
	session *sessionHandle,
	value *wirev1.Execution,
) (*executionHandle, error) {
	if value == nil || value.GetId().GetValue() == "" {
		return nil, &allocationError{cause: errors.New("Wire server returned an invalid execution")}
	}

	return &executionHandle{
		client:  client,
		session: session,
		id:      value.GetId().GetValue(),
		close:   &closeState{},
	}, nil
}

// Watch opens an ordered event stream tied to ctx and the logical connection.
// Its first event contains the current remote snapshot.
func (e *executionHandle) Watch(ctx context.Context) (*executionEvents, error) {
	if err := e.checkOpen(); err != nil {
		return nil, err
	}

	watchCtx, cancel := e.client.watchContext(ctx)

	stream, err := e.client.executionClient.WatchExecution(watchCtx, &wirev1.WatchExecutionRequest{
		ConnectionId: e.client.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: e.id},
	})
	if err != nil {
		cancel()

		return nil, decodeError(err)
	}

	return &executionEvents{stream: stream, cancel: cancel}, nil
}

// Wait observes execution events until the remote execution reaches a terminal
// state. A failed execution returns *failure.Failure, while remote cancellation
// returns ErrExecutionCancelled. Caller cancellation returns the waiting
// context's error. Wait does not release the execution or retain mutable
// snapshot state.
func (e *executionHandle) Wait(ctx context.Context) (api.Output, error) {
	events, err := e.Watch(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return api.Output{}, ctxErr
		}

		return api.Output{}, err
	}

	for {
		event, receiveErr := events.Recv()
		if receiveErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return api.Output{}, ctxErr
			}

			return api.Output{}, receiveErr
		}

		if !event.Snapshot.State.Terminal() {
			continue
		}

		output := executionOutput(event.Snapshot)
		switch event.Snapshot.State {
		case execution.StateCompleted:
			if event.Snapshot.Output == nil {
				return api.Output{}, errors.New("Wire server returned a completed execution without output")
			}

			return output, nil
		case execution.StateFailed:
			if event.Snapshot.Failure == nil {
				return output, errors.New("Wire server returned a failed execution without failure details")
			}

			return output, event.Snapshot.Failure
		case execution.StateCancelled:
			return output, ErrExecutionCancelled
		}
	}
}

// Close commits cancellation and remote execution cleanup. Concurrent and
// repeated calls observe one retained release result.
func (e *executionHandle) Close(ctx context.Context) error {
	if e == nil || e.client == nil || e.id == "" || e.close == nil {
		return ErrClosed
	}

	if e.close.Begin() {
		go settleHandleClose(ctx, "execution", e.close, e.release)
	}

	return e.close.Wait(ctx)
}

func (e *executionHandle) checkOpen() error {
	if e == nil || e.client == nil || e.id == "" || e.close == nil || e.close.Started() {
		return ErrClosed
	}

	if e.session != nil {
		return e.session.checkOpen()
	}

	return e.client.checkOpen()
}

func (e *executionHandle) release(ctx context.Context) error {
	if e.session != nil {
		if closing, err := e.session.ancestorCloseResult(ctx); closing {
			return err
		}
	} else if closing, err := e.client.closeResult(ctx); closing {
		return err
	}

	if err := e.client.checkOpen(); err != nil {
		return err
	}

	_, err := e.client.executionClient.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{
		ConnectionId: e.client.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: e.id},
	})

	return decodeError(err)
}

// waitAndRelease is the adapter's one-shot invocation lifecycle. Release itself
// cancels running work and waits for teardown; a separate Cancel RPC is redundant.
func (e *executionHandle) waitAndRelease(ctx context.Context) (api.Output, error) {
	output, waitErr := e.Wait(ctx)

	// The ID is known: a failed release is retained on this handle and does
	// not invalidate its session, plan, or logical runtime.
	closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, e.Close)

	return output, errors.Join(waitErr, closeErr)
}
