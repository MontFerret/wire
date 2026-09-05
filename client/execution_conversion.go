package client

import (
	"fmt"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/execution"
)

func executionOutput(snapshot execution.Snapshot) api.Output {
	if snapshot.Output == nil {
		return api.Output{}
	}

	return api.Output{
		ContentType: snapshot.Output.ContentType,
		Content:     append([]byte(nil), snapshot.Output.Content...),
	}
}

func convertOutput(value *wirev1.Output) *api.Output {
	if value == nil {
		return nil
	}

	return &api.Output{ContentType: value.GetContentType(), Content: append([]byte(nil), value.GetContent()...)}
}

func convertExecutionSnapshot(value *wirev1.Execution) (execution.Snapshot, error) {
	state, err := convertExecutionState(value.GetState())
	if err != nil {
		return execution.Snapshot{}, err
	}

	terminalFailure, err := convertFailure(value.GetFailure())
	if err != nil {
		return execution.Snapshot{}, err
	}

	return execution.Snapshot{
		State:   state,
		Output:  convertOutput(value.GetOutput()),
		Failure: terminalFailure,
	}, nil
}

func convertExecutionState(value wirev1.ExecutionState) (execution.State, error) {
	switch value {
	case wirev1.ExecutionState_EXECUTION_STATE_RUNNING:
		return execution.StateRunning, nil
	case wirev1.ExecutionState_EXECUTION_STATE_COMPLETED:
		return execution.StateCompleted, nil
	case wirev1.ExecutionState_EXECUTION_STATE_FAILED:
		return execution.StateFailed, nil
	case wirev1.ExecutionState_EXECUTION_STATE_CANCELLED:
		return execution.StateCancelled, nil
	default:
		return 0, fmt.Errorf("Wire server returned an invalid execution state: %d", value)
	}
}

func convertExecutionEvent(value *wirev1.WatchExecutionResponse) (execution.Event, error) {
	snapshot, err := convertExecutionSnapshot(value.GetExecution())
	if err != nil {
		return execution.Event{}, err
	}

	return execution.Event{
		Sequence: value.GetSequence(),
		Snapshot: snapshot,
	}, nil
}
