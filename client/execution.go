package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/execution"
)

// Execution is one asynchronous remote operation owned by a Client, Plan, or
// durable Session.
type Execution struct {
	client  *Client
	plan    *Plan
	session *sessionHandle
	id      string
	close   *closeState
}

func newExecutionHandle(
	client *Client,
	plan *Plan,
	session *sessionHandle,
	value *wirev1.Execution,
) (*Execution, error) {
	if value == nil || value.GetId().GetValue() == "" {
		return nil, &allocationError{cause: errors.New("Wire server returned an invalid execution")}
	}

	return &Execution{
		client:  client,
		plan:    plan,
		session: session,
		id:      value.GetId().GetValue(),
		close:   &closeState{},
	}, nil
}

// Cancel requests execution cancellation. The ordered terminal cancellation
// snapshot remains observable through Watch.
func (e *Execution) Cancel(ctx context.Context) error {
	if err := e.checkOpen(); err != nil {
		return err
	}

	_, err := e.client.executionClient.CancelExecution(ctx, &wirev1.CancelExecutionRequest{
		ConnectionId: e.client.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: e.id},
	})

	return decodeError(err)
}

// Watch opens an ordered event stream tied to both ctx and the Client's
// logical lifecycle. Its first event contains the current remote snapshot.
func (e *Execution) Watch(ctx context.Context) (*ExecutionEvents, error) {
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

	return &ExecutionEvents{stream: stream, cancel: cancel}, nil
}

// Wait observes execution events until the remote execution reaches a terminal
// state. A failed execution returns *failure.Failure, while remote cancellation
// returns ErrExecutionCancelled. Caller cancellation returns the waiting
// context's error. Wait does not release the execution or retain mutable
// snapshot state.
func (e *Execution) Wait(ctx context.Context) (Output, error) {
	events, err := e.Watch(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Output{}, ctxErr
		}

		return Output{}, err
	}

	for {
		event, receiveErr := events.Recv()
		if receiveErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return Output{}, ctxErr
			}

			return Output{}, receiveErr
		}

		if !event.Snapshot.State.Terminal() {
			continue
		}

		output := executionOutput(event.Snapshot)
		switch event.Snapshot.State {
		case execution.StateCompleted:
			if event.Snapshot.Output == nil {
				return Output{}, errors.New("Wire server returned a completed execution without output")
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
func (e *Execution) Close(ctx context.Context) error {
	if e == nil || e.client == nil || e.id == "" || e.close == nil {
		return ErrClosed
	}

	if e.close.Begin() {
		go settleHandleClose(ctx, "execution", e.close, e.release)
	}

	return e.close.Wait(ctx)
}

func (e *Execution) checkOpen() error {
	if e == nil || e.client == nil || e.id == "" || e.close == nil || e.close.Started() {
		return ErrClosed
	}

	if e.session != nil {
		return e.session.checkOpen()
	}

	if e.plan == nil {
		return e.client.checkOpen()
	}

	return e.plan.checkOpen()
}

func (e *Execution) release(ctx context.Context) error {
	if e.session != nil {
		if closing, err := e.session.ancestorCloseResult(ctx); closing {
			return err
		}
	} else if e.plan != nil {
		if closing, err := e.plan.ancestorCloseResult(ctx); closing {
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
func (e *Execution) waitAndRelease(ctx context.Context) (Output, error) {
	output, waitErr := e.Wait(ctx)

	// The ID is known: a failed release is retained on this handle and does
	// not invalidate its Session, Plan, or logical Runtime.
	closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, e.Close)

	return output, errors.Join(waitErr, closeErr)
}
