package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/execution"
)

type (
	// ExecuteOptions controls encoded execution output.
	ExecuteOptions struct {
		OutputContentType string
	}

	// Execution is one remote execution owned by its Plan.
	Execution struct {
		client *Client
		plan   *Plan
		id     string
		close  *closeState
	}

	// ExecutionEvents receives the current execution snapshot followed by ordered
	// state changes until the terminal event or stream cancellation.
	ExecutionEvents struct {
		stream wirev1.ExecutionService_WatchExecutionClient
		cancel context.CancelFunc
	}
)

// Execute publishes a remote execution of this plan. Output remains the Unified API
// encoded content-type and byte contract.
func (p *Plan) Execute(ctx context.Context, parameters Parameters, options ExecuteOptions) (*Execution, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.executionClient.Execute(ctx, &wirev1.ExecuteRequest{
		ConnectionId:      p.client.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: p.id},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return nil, decodeError(err)
	}

	value := response.GetExecution()
	if value == nil || value.GetId().GetValue() == "" {
		return nil, errors.New("Wire server returned an invalid execution")
	}

	return &Execution{client: p.client, plan: p, id: value.GetId().GetValue(), close: &closeState{}}, nil
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
func (e *Execution) Wait(ctx context.Context) (api.Output, error) {
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
func (e *Execution) Close(ctx context.Context) error {
	if e == nil || e.client == nil || e.plan == nil || e.id == "" || e.close == nil {
		return ErrClosed
	}

	if e.close.Begin() {
		go settleHandleClose(ctx, "execution", e.close, e.release)
	}

	return e.close.Wait(ctx)
}

// Recv blocks for the next ordered execution event. It releases the local
// stream when a terminal event or error is observed.
func (events *ExecutionEvents) Recv() (execution.Event, error) {
	if events == nil || events.stream == nil {
		return execution.Event{}, errors.New("execution event receiver is nil")
	}

	value, err := events.stream.Recv()
	if err != nil {
		events.cancel()

		return execution.Event{}, decodeError(err)
	}

	if value.GetExecution() == nil {
		events.cancel()

		return execution.Event{}, fmt.Errorf("Wire server returned an empty execution event")
	}

	event, err := convertExecutionEvent(value)
	if err != nil {
		events.cancel()

		return execution.Event{}, err
	}

	if event.Snapshot.State.Terminal() {
		events.cancel()
	}

	return event, nil
}

func (e *Execution) checkOpen() error {
	if e == nil || e.client == nil || e.plan == nil || e.id == "" || e.close == nil || e.close.Started() {
		return ErrClosed
	}

	return e.plan.checkOpen()
}

func (e *Execution) release(ctx context.Context) error {
	if closing, err := e.plan.ancestorCloseResult(ctx); closing {
		return err
	}

	return e.client.releaseExecution(ctx, e.id)
}

func executionOutput(snapshot execution.Snapshot) api.Output {
	if snapshot.Output == nil {
		return api.Output{}
	}

	return api.Output{
		ContentType: snapshot.Output.ContentType,
		Content:     append([]byte(nil), snapshot.Output.Content...),
	}
}

func (c *Client) releaseExecution(ctx context.Context, id string) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	_, err := c.executionClient.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{
		ConnectionId: c.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: id},
	})

	return decodeError(err)
}
