package client

import (
	"fmt"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func convertDebugSessionSnapshot(value *wirev1.DebugSession) (DebugSessionSnapshot, error) {
	if value == nil {
		return DebugSessionSnapshot{}, invalidDebuggerResponse("debug session is missing")
	}

	state, err := convertDebugState(value.GetState())
	if err != nil {
		return DebugSessionSnapshot{}, err
	}

	stopReason, err := convertDebugStopReason(value.GetStopReason())
	if err != nil {
		return DebugSessionSnapshot{}, err
	}

	location, err := convertSourceRange(value.GetLocation())
	if err != nil {
		return DebugSessionSnapshot{}, err
	}

	hitIDs := make([]debugger.BreakpointID, len(value.GetHitBreakpointIds()))
	for i, id := range value.GetHitBreakpointIds() {
		converted, err := debuggerIDFromProto[debugger.BreakpointID](id, "hit breakpoint ID", false)
		if err != nil {
			return DebugSessionSnapshot{}, err
		}

		hitIDs[i] = converted
	}

	return DebugSessionSnapshot{
		State:            state,
		StopReason:       stopReason,
		Location:         location,
		HitBreakpointIDs: hitIDs,
		Output:           convertOutput(value.GetOutput()),
		Failure:          convertFailure(value.GetFailure()),
	}, nil
}

func convertDebugState(value wirev1.DebugState) (DebugState, error) {
	switch value {
	case wirev1.DebugState_DEBUG_STATE_CREATED:
		return DebugCreated, nil
	case wirev1.DebugState_DEBUG_STATE_RUNNING:
		return DebugRunning, nil
	case wirev1.DebugState_DEBUG_STATE_STOPPED:
		return DebugStopped, nil
	case wirev1.DebugState_DEBUG_STATE_COMPLETED:
		return DebugCompleted, nil
	case wirev1.DebugState_DEBUG_STATE_FAILED:
		return DebugFailed, nil
	case wirev1.DebugState_DEBUG_STATE_TERMINATED:
		return DebugTerminated, nil
	default:
		return 0, invalidDebuggerResponse("unknown debug state %d", value)
	}
}

func convertDebugStopReason(value wirev1.DebugStopReason) (debugger.Reason, error) {
	switch value {
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED:
		return "", nil
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY:
		return debugger.ReasonEntry, nil
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT:
		return debugger.ReasonBreakpoint, nil
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP:
		return debugger.ReasonStep, nil
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE:
		return debugger.ReasonPause, nil
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR:
		return debugger.ReasonRuntimeError, nil
	default:
		return "", invalidDebuggerResponse("unknown stop reason %d", value)
	}
}

func convertDebugEvent(value *wirev1.WatchDebugResponse) (DebugEvent, error) {
	result := DebugEvent{Sequence: value.GetSequence()}
	var snapshot *wirev1.DebugSession

	switch payload := value.GetPayload().(type) {
	case *wirev1.WatchDebugResponse_Started:
		result.Kind = DebugEventStarted
		snapshot = payload.Started.GetSession()
	case *wirev1.WatchDebugResponse_Continued:
		result.Kind = DebugEventContinued
		snapshot = payload.Continued.GetSession()
	case *wirev1.WatchDebugResponse_Stopped:
		result.Kind = DebugEventStopped
		snapshot = payload.Stopped.GetSession()
	case *wirev1.WatchDebugResponse_Completed:
		result.Kind = DebugEventCompleted
		snapshot = payload.Completed.GetSession()
	case *wirev1.WatchDebugResponse_Failed:
		result.Kind = DebugEventFailed
		snapshot = payload.Failed.GetSession()
	case *wirev1.WatchDebugResponse_Terminated:
		result.Kind = DebugEventTerminated
		snapshot = payload.Terminated.GetSession()
	default:
		return DebugEvent{}, invalidDebuggerResponse("event payload is missing")
	}

	converted, err := convertDebugSessionSnapshot(snapshot)
	if err != nil {
		return DebugEvent{}, err
	}
	result.Snapshot = converted

	return result, nil
}

func convertSourceLocation(value *wirev1.SourceLocation) (*source.Location, error) {
	if value == nil {
		return nil, nil
	}

	if value.GetLine() <= 0 || value.GetColumn() < 0 {
		return nil, invalidDebuggerResponse("source location is invalid")
	}

	return &source.Location{
		Position: source.Position{Line: int(value.GetLine()), Column: int(value.GetColumn())},
		File:     value.GetFile(),
	}, nil
}

func convertSourceRange(value *wirev1.SourceLocation) (*source.Range, error) {
	location, err := convertSourceLocation(value)
	if err != nil || location == nil {
		return nil, err
	}

	return &source.Range{Location: *location}, nil
}

func debuggerIDFromProto[T ~int](value uint64, name string, zeroAllowed bool) (T, error) {
	if value == 0 {
		if zeroAllowed {
			return 0, nil
		}

		return 0, invalidDebuggerResponse("%s must be positive", name)
	}

	if value > uint64(^uint(0)>>1) {
		return 0, invalidDebuggerResponse("%s is out of range", name)
	}

	return T(value), nil
}

func invalidDebuggerResponse(format string, args ...any) error {
	return fmt.Errorf("Wire server returned an invalid debugger response: %s", fmt.Sprintf(format, args...))
}
