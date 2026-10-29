package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
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

// Wait consumes the execution's one-shot output and releases its resources.
// Available partial content accompanies terminal and cleanup failures.
func (e *executionHandle) Wait(ctx context.Context) (*api.Content, error) {
	output, err := e.openOutput(ctx, nil)
	if err != nil {
		return nil, err
	}

	return output.Collect(ctx)
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
