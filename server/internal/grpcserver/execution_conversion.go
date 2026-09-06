package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/server/internal/core"
)

func execution(id core.ExecutionID, value wireexecution.Snapshot) (*wirev1.Execution, error) {
	state, err := executionState(value.State)
	if err != nil {
		return nil, err
	}

	convertedFailure, err := failure(value.Failure)
	if err != nil {
		return nil, err
	}

	return &wirev1.Execution{
		Id:      &wirev1.ExecutionId{Value: string(id)},
		State:   state,
		Output:  output(value.Output),
		Failure: convertedFailure,
	}, nil
}

func executionState(value wireexecution.State) (wirev1.ExecutionState, error) {
	switch value {
	case wireexecution.StateRunning:
		return wirev1.ExecutionState_EXECUTION_STATE_RUNNING, nil
	case wireexecution.StateCompleted:
		return wirev1.ExecutionState_EXECUTION_STATE_COMPLETED, nil
	case wireexecution.StateFailed:
		return wirev1.ExecutionState_EXECUTION_STATE_FAILED, nil
	case wireexecution.StateCancelled:
		return wirev1.ExecutionState_EXECUTION_STATE_CANCELLED, nil
	}

	return 0, runtimeConversionError("runtime returned an invalid execution state")
}

func executionEvent(id core.ExecutionID, value wireexecution.Event) (*wirev1.WatchExecutionResponse, error) {
	snapshot, err := execution(id, value.Snapshot)
	if err != nil {
		return nil, err
	}

	return &wirev1.WatchExecutionResponse{
		Sequence:  value.Sequence,
		Execution: snapshot,
	}, nil
}
