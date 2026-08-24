package client

import (
	"context"
	"errors"
	"fmt"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	// ExecuteOptions controls encoded execution output.
	ExecuteOptions struct {
		OutputContentType string
	}

	// Output preserves Ferret's encoded content-type and byte abstraction.
	Output struct {
		ContentType string
		Content     []byte
	}

	// ExecutionState describes the lifecycle state in an ExecutionSnapshot.
	ExecutionState uint8

	// ExecutionSnapshot is the state published for one remote execution event.
	ExecutionSnapshot struct {
		State   ExecutionState
		Output  *Output
		Failure *Failure
	}

	// ExecutionEvent carries an ordered execution snapshot.
	ExecutionEvent struct {
		Sequence uint64
		Snapshot ExecutionSnapshot
	}

	// Execution is one remote execution owned by its Plan.
	Execution struct {
		client *Client
		plan   *Plan
		id     string
		close  *lifecycle.Close
	}

	// ExecutionEvents receives the current execution snapshot followed by ordered
	// state changes until the terminal event or stream cancellation.
	ExecutionEvents struct {
		stream wirev1.ExecutionService_WatchExecutionClient
		cancel context.CancelFunc
	}
)

// Execution lifecycle states.
const (
	ExecutionRunning ExecutionState = iota + 1
	ExecutionCompleted
	ExecutionFailed
	ExecutionCancelled
)

// Execute publishes a remote execution of this plan. Output remains Ferret's
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

	return &Execution{client: p.client, plan: p, id: value.GetId().GetValue(), close: &lifecycle.Close{}}, nil
}

func (e *Execution) checkOpen() error {
	if e == nil || e.client == nil || e.plan == nil || e.id == "" || e.close == nil || e.close.Started() {
		return ErrClosed
	}

	return e.plan.checkOpen()
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

func (e *Execution) release(ctx context.Context) error {
	if closing, err := e.plan.ancestorCloseResult(ctx); closing {
		return err
	}

	return e.client.releaseExecution(ctx, e.id)
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
// state. A failed execution returns *Failure, while remote cancellation returns
// ErrExecutionCancelled. Caller cancellation returns the waiting context's
// error. Wait does not release the execution or retain mutable snapshot state.
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

		output := event.Snapshot.output()
		switch event.Snapshot.State {
		case ExecutionCompleted:
			if event.Snapshot.Output == nil {
				return Output{}, errors.New("Wire server returned a completed execution without output")
			}

			return output, nil
		case ExecutionFailed:
			if event.Snapshot.Failure == nil {
				return output, errors.New("Wire server returned a failed execution without failure details")
			}

			return output, event.Snapshot.Failure
		case ExecutionCancelled:
			return output, ErrExecutionCancelled
		}
	}
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
	if event.Snapshot.State.Terminal() {
		events.cancel()
	}

	return event, nil
}

// Terminal reports whether the execution has reached a final state.
func (state ExecutionState) Terminal() bool {
	switch state {
	case ExecutionCompleted, ExecutionFailed, ExecutionCancelled:
		return true
	default:
		return false
	}
}

func convertOutput(value *wirev1.Output) *Output {
	if value == nil {
		return nil
	}

	return &Output{ContentType: value.GetContentType(), Content: append([]byte(nil), value.GetContent()...)}
}

func convertExecutionSnapshot(value *wirev1.Execution) ExecutionSnapshot {
	return ExecutionSnapshot{
		State:   convertExecutionState(value.GetState()),
		Output:  convertOutput(value.GetOutput()),
		Failure: convertFailure(value.GetFailure()),
	}
}

func convertExecutionState(value wirev1.ExecutionState) ExecutionState {
	switch value {
	case wirev1.ExecutionState_EXECUTION_STATE_RUNNING:
		return ExecutionRunning
	case wirev1.ExecutionState_EXECUTION_STATE_COMPLETED:
		return ExecutionCompleted
	case wirev1.ExecutionState_EXECUTION_STATE_FAILED:
		return ExecutionFailed
	case wirev1.ExecutionState_EXECUTION_STATE_CANCELLED:
		return ExecutionCancelled
	default:
		return 0
	}
}

func convertExecutionEvent(value *wirev1.WatchExecutionResponse) ExecutionEvent {
	result := ExecutionEvent{Sequence: value.GetSequence()}

	switch payload := value.GetPayload().(type) {
	case *wirev1.WatchExecutionResponse_Started:
		result.Snapshot = convertExecutionSnapshot(payload.Started.GetExecution())
	case *wirev1.WatchExecutionResponse_Completed:
		result.Snapshot = convertExecutionSnapshot(payload.Completed.GetExecution())
	case *wirev1.WatchExecutionResponse_Failed:
		result.Snapshot = convertExecutionSnapshot(payload.Failed.GetExecution())
	case *wirev1.WatchExecutionResponse_Cancelled:
		result.Snapshot = convertExecutionSnapshot(payload.Cancelled.GetExecution())
	}

	return result
}

func (snapshot ExecutionSnapshot) output() Output {
	if snapshot.Output == nil {
		return Output{}
	}

	return Output{
		ContentType: snapshot.Output.ContentType,
		Content:     append([]byte(nil), snapshot.Output.Content...),
	}
}
