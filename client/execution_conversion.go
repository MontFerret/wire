package client

import wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"

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
	return ExecutionEvent{
		Sequence: value.GetSequence(),
		Snapshot: convertExecutionSnapshot(value.GetExecution()),
	}
}
