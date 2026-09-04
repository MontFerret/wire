package client

import (
	"fmt"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wireruntime "github.com/MontFerret/wire/pkg/runtime"
)

func convertOutput(value *wirev1.Output) *api.Output {
	if value == nil {
		return nil
	}

	return &api.Output{ContentType: value.GetContentType(), Content: append([]byte(nil), value.GetContent()...)}
}

func convertExecutionSnapshot(value *wirev1.Execution) (wireruntime.Snapshot, error) {
	state, err := convertExecutionState(value.GetState())
	if err != nil {
		return wireruntime.Snapshot{}, err
	}

	terminalFailure, err := convertFailure(value.GetFailure())
	if err != nil {
		return wireruntime.Snapshot{}, err
	}

	return wireruntime.Snapshot{
		State:   state,
		Output:  convertOutput(value.GetOutput()),
		Failure: terminalFailure,
	}, nil
}

func convertExecutionState(value wirev1.ExecutionState) (wireruntime.State, error) {
	switch value {
	case wirev1.ExecutionState_EXECUTION_STATE_RUNNING:
		return wireruntime.StateRunning, nil
	case wirev1.ExecutionState_EXECUTION_STATE_COMPLETED:
		return wireruntime.StateCompleted, nil
	case wirev1.ExecutionState_EXECUTION_STATE_FAILED:
		return wireruntime.StateFailed, nil
	case wirev1.ExecutionState_EXECUTION_STATE_CANCELLED:
		return wireruntime.StateCancelled, nil
	default:
		return 0, fmt.Errorf("Wire server returned an invalid execution state: %d", value)
	}
}

func convertExecutionEvent(value *wirev1.WatchExecutionResponse) (wireruntime.Event, error) {
	snapshot, err := convertExecutionSnapshot(value.GetExecution())
	if err != nil {
		return wireruntime.Event{}, err
	}

	return wireruntime.Event{
		Sequence: value.GetSequence(),
		Snapshot: snapshot,
	}, nil
}
