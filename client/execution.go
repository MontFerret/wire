package client

import (
	"context"
	"errors"
	"fmt"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// ExecutionEvents receives the current execution snapshot followed by ordered
// state changes until the terminal event or stream cancellation.
type ExecutionEvents struct {
	stream wirev1.ExecutionService_WatchExecutionClient
	cancel context.CancelFunc
}

// Execute publishes a connection-owned execution. Output remains Ferret's
// encoded content-type and byte contract.
func (c *Client) Execute(ctx context.Context, id PlanID, parameters Parameters, options ExecuteOptions) (Execution, error) {
	if err := c.checkOpen(); err != nil {
		return Execution{}, err
	}

	converted, err := encodeParameters(parameters)
	if err != nil {
		return Execution{}, err
	}

	response, err := c.executionClient.Execute(ctx, &wirev1.ExecuteRequest{
		ConnectionId:      c.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: string(id)},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return Execution{}, decodeError(err)
	}

	if response.GetExecution() == nil {
		return Execution{}, errors.New("Wire server returned no execution")
	}

	return convertExecution(response.GetExecution()), nil
}

// CancelExecution requests cancellation and returns the current snapshot. The
// ordered terminal cancellation is observable through WatchExecution.
func (c *Client) CancelExecution(ctx context.Context, id ExecutionID) (Execution, error) {
	if err := c.checkOpen(); err != nil {
		return Execution{}, err
	}

	response, err := c.executionClient.CancelExecution(ctx, &wirev1.CancelExecutionRequest{
		ConnectionId: c.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: string(id)},
	})
	if err != nil {
		return Execution{}, decodeError(err)
	}

	return convertExecution(response.GetExecution()), nil
}

// ReleaseExecution commits cancellation and cleanup. The ID becomes stale when
// cleanup completes.
func (c *Client) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	_, err := c.executionClient.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{
		ConnectionId: c.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: string(id)},
	})

	return decodeError(err)
}

// WatchExecution opens an ordered watch tied to both ctx and the Client's
// logical lifecycle.
func (c *Client) WatchExecution(ctx context.Context, id ExecutionID) (*ExecutionEvents, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	watchCtx, cancel := c.watchContext(ctx)
	stream, err := c.executionClient.WatchExecution(watchCtx, &wirev1.WatchExecutionRequest{
		ConnectionId: c.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: string(id)},
	})
	if err != nil {
		cancel()

		return nil, decodeError(err)
	}

	return &ExecutionEvents{stream: stream, cancel: cancel}, nil
}

// Recv blocks for the next ordered execution event. It releases the local
// stream when a terminal event or error is observed.
func (events *ExecutionEvents) Recv() (ExecutionEvent, error) {
	if events == nil || events.stream == nil {
		return ExecutionEvent{}, errors.New("execution event receiver is nil")
	}

	value, err := events.stream.Recv()
	if err != nil {
		events.cancel()

		return ExecutionEvent{}, decodeError(err)
	}

	if value.GetPayload() == nil {
		events.cancel()

		return ExecutionEvent{}, fmt.Errorf("Wire server returned an empty execution event")
	}

	event := convertExecutionEvent(value)
	if event.Kind == ExecutionEventCompleted || event.Kind == ExecutionEventFailed || event.Kind == ExecutionEventCancelled {
		events.cancel()
	}

	return event, nil
}
