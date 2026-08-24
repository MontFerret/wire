package client

import wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"

func convertDebugSessionSnapshot(value *wirev1.DebugSession) DebugSessionSnapshot {
	return DebugSessionSnapshot{
		State:            convertDebugState(value.GetState()),
		StopReason:       convertDebugStopReason(value.GetStopReason()),
		Location:         convertLocation(value.GetLocation()),
		HitBreakpointIDs: append([]uint64(nil), value.GetHitBreakpointIds()...),
		Output:           convertOutput(value.GetOutput()),
		Failure:          convertFailure(value.GetFailure()),
	}
}

func convertDebugState(value wirev1.DebugState) DebugState {
	switch value {
	case wirev1.DebugState_DEBUG_STATE_CREATED:
		return DebugCreated
	case wirev1.DebugState_DEBUG_STATE_RUNNING:
		return DebugRunning
	case wirev1.DebugState_DEBUG_STATE_STOPPED:
		return DebugStopped
	case wirev1.DebugState_DEBUG_STATE_COMPLETED:
		return DebugCompleted
	case wirev1.DebugState_DEBUG_STATE_FAILED:
		return DebugFailed
	case wirev1.DebugState_DEBUG_STATE_TERMINATED:
		return DebugTerminated
	default:
		return 0
	}
}

func convertDebugStopReason(value wirev1.DebugStopReason) DebugStopReason {
	switch value {
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY:
		return DebugStopEntry
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT:
		return DebugStopBreakpoint
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP:
		return DebugStopStep
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE:
		return DebugStopPause
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR:
		return DebugStopRuntimeError
	default:
		return DebugStopNone
	}
}

func convertDebugEvent(value *wirev1.WatchDebugResponse) DebugEvent {
	result := DebugEvent{Sequence: value.GetSequence()}

	switch payload := value.GetPayload().(type) {
	case *wirev1.WatchDebugResponse_Started:
		result.Kind = DebugEventStarted
		result.Snapshot = convertDebugSessionSnapshot(payload.Started.GetSession())
	case *wirev1.WatchDebugResponse_Continued:
		result.Kind = DebugEventContinued
		result.Snapshot = convertDebugSessionSnapshot(payload.Continued.GetSession())
	case *wirev1.WatchDebugResponse_Stopped:
		result.Kind = DebugEventStopped
		result.Snapshot = convertDebugSessionSnapshot(payload.Stopped.GetSession())
	case *wirev1.WatchDebugResponse_Completed:
		result.Kind = DebugEventCompleted
		result.Snapshot = convertDebugSessionSnapshot(payload.Completed.GetSession())
	case *wirev1.WatchDebugResponse_Failed:
		result.Kind = DebugEventFailed
		result.Snapshot = convertDebugSessionSnapshot(payload.Failed.GetSession())
	case *wirev1.WatchDebugResponse_Terminated:
		result.Kind = DebugEventTerminated
		result.Snapshot = convertDebugSessionSnapshot(payload.Terminated.GetSession())
	}

	return result
}
