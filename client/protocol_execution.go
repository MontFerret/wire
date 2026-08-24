package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

type protocolExecutionEvents struct {
	stream wirev1.ExecutionService_WatchExecutionClient
}

func (p *protocolClient) execute(ctx context.Context, planID string, parameters Parameters, options ExecuteOptions) (string, error) {
	converted, err := encodeParameters(parameters)
	if err != nil {
		return "", err
	}

	response, err := p.executionClient.Execute(ctx, &wirev1.ExecuteRequest{
		ConnectionId:      p.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: planID},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return "", decodeError(err)
	}

	value := response.GetExecution()
	if value == nil || value.GetId().GetValue() == "" {
		return "", errors.New("Wire server returned an invalid execution")
	}

	return value.GetId().GetValue(), nil
}

func (p *protocolClient) cancelExecution(ctx context.Context, id string) error {
	_, err := p.executionClient.CancelExecution(ctx, &wirev1.CancelExecutionRequest{
		ConnectionId: p.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: id},
	})

	return decodeError(err)
}

func (p *protocolClient) watchExecution(ctx context.Context, id string) (*protocolExecutionEvents, error) {
	stream, err := p.executionClient.WatchExecution(ctx, &wirev1.WatchExecutionRequest{
		ConnectionId: p.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: id},
	})
	if err != nil {
		return nil, decodeError(err)
	}

	if stream == nil {
		return nil, errors.New("Wire server returned no execution event stream")
	}

	return &protocolExecutionEvents{stream: stream}, nil
}

func (p *protocolClient) releaseExecution(ctx context.Context, id string) error {
	_, err := p.executionClient.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{
		ConnectionId: p.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: id},
	})

	return decodeError(err)
}

func (events *protocolExecutionEvents) recv() (ExecutionEvent, error) {
	value, err := events.stream.Recv()
	if err != nil {
		return ExecutionEvent{}, decodeError(err)
	}

	if value.GetPayload() == nil {
		return ExecutionEvent{}, errors.New("Wire server returned an empty execution event")
	}

	return convertExecutionEvent(value), nil
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
