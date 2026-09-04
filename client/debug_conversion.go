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

	depth, err := debuggerIntFromProto(value.GetDepth(), "debug depth", true)
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

	failure, err := convertFailure(value.GetFailure())
	if err != nil {
		return DebugSessionSnapshot{}, err
	}

	return DebugSessionSnapshot{
		State:            state,
		StopReason:       stopReason,
		Location:         location,
		HitBreakpointIDs: hitIDs,
		Depth:            depth,
		Output:           convertOutput(value.GetOutput()),
		Failure:          failure,
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
	kind, err := convertDebugEventKind(value.GetKind())
	if err != nil {
		return DebugEvent{}, err
	}

	snapshot, err := convertDebugSessionSnapshot(value.GetSession())
	if err != nil {
		return DebugEvent{}, err
	}

	return DebugEvent{Sequence: value.GetSequence(), Kind: kind, Snapshot: snapshot}, nil
}

func convertDebugEventKind(value wirev1.DebugEventKind) (DebugEventKind, error) {
	switch value {
	case wirev1.DebugEventKind_DEBUG_EVENT_KIND_STARTED:
		return DebugEventStarted, nil
	case wirev1.DebugEventKind_DEBUG_EVENT_KIND_CONTINUED:
		return DebugEventContinued, nil
	case wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED:
		return DebugEventStopped, nil
	case wirev1.DebugEventKind_DEBUG_EVENT_KIND_COMPLETED:
		return DebugEventCompleted, nil
	case wirev1.DebugEventKind_DEBUG_EVENT_KIND_FAILED:
		return DebugEventFailed, nil
	case wirev1.DebugEventKind_DEBUG_EVENT_KIND_TERMINATED:
		return DebugEventTerminated, nil
	case wirev1.DebugEventKind_DEBUG_EVENT_KIND_CREATED:
		return DebugEventCreated, nil
	default:
		return 0, invalidDebuggerResponse("unknown debug event kind %d", value)
	}
}

func convertSourceLocation(value *wirev1.Location) (*source.Location, error) {
	if value == nil {
		return nil, nil
	}

	if value.GetSourceName() == "" {
		return nil, invalidDebuggerResponse("source location source name is missing")
	}

	position := value.GetPosition()
	if position == nil || position.GetLine() <= 0 || position.GetColumn() < 0 {
		return nil, invalidDebuggerResponse("source location is invalid")
	}

	line, err := debuggerIntFromProto(position.GetLine(), "source line", false)
	if err != nil {
		return nil, err
	}

	column, err := debuggerIntFromProto(position.GetColumn(), "source column", true)
	if err != nil {
		return nil, err
	}

	return &source.Location{
		Position:   source.Position{Line: line, Column: column},
		SourceName: value.GetSourceName(),
	}, nil
}

func convertSourceRange(value *wirev1.Range) (*source.Range, error) {
	if value == nil {
		return nil, nil
	}

	location, err := convertSourceLocation(value.GetLocation())
	if err != nil {
		return nil, err
	}

	if location == nil || value.GetSpan() == nil {
		return nil, invalidDebuggerResponse("source range is incomplete")
	}

	start, err := debuggerIntFromProto(value.GetSpan().GetStart(), "source span start", true)
	if err != nil {
		return nil, err
	}

	end, err := debuggerIntFromProto(value.GetSpan().GetEnd(), "source span end", true)
	if err != nil {
		return nil, err
	}

	if end < start {
		return nil, invalidDebuggerResponse("source span end precedes start")
	}

	return &source.Range{
		Location: *location,
		Span:     source.Span{Start: start, End: end},
	}, nil
}

func debuggerIntFromProto(value int64, name string, zeroAllowed bool) (int, error) {
	if value < 0 || (!zeroAllowed && value == 0) || uint64(value) > uint64(^uint(0)>>1) {
		return 0, invalidDebuggerResponse("%s is out of range", name)
	}

	return int(value), nil
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
