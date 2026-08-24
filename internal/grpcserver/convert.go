package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

func runtimeInfo(value core.RuntimeInfo) *wirev1.RuntimeInfo {
	result := &wirev1.RuntimeInfo{
		ApiIdentity:   value.APIIdentity,
		WireVersion:   value.WireVersion,
		FerretVersion: value.FerretVersion,
		Capabilities: []wirev1.Capability{
			wirev1.Capability_CAPABILITY_EXECUTION,
			wirev1.Capability_CAPABILITY_DEBUGGING,
			wirev1.Capability_CAPABILITY_CANCELLATION,
		},
	}

	if value.RuntimeIdentity != (core.RuntimeIdentity{}) {
		result.RuntimeIdentity = &wirev1.RuntimeIdentity{
			Name:       value.RuntimeIdentity.Name,
			Version:    value.RuntimeIdentity.Version,
			InstanceId: value.RuntimeIdentity.InstanceID,
		}
	}

	return result
}

func diagnostics(values []core.Diagnostic) []*wirev1.Diagnostic {
	result := make([]*wirev1.Diagnostic, len(values))
	for i, value := range values {
		spans := make([]*wirev1.DiagnosticSpan, len(value.Spans))
		for j, span := range value.Spans {
			spans[j] = &wirev1.DiagnosticSpan{
				StartByte: span.Start,
				EndByte:   span.End,
				Label:     span.Label,
				Primary:   span.Primary,
			}
		}
		result[i] = &wirev1.Diagnostic{
			Kind:           value.Kind,
			Message:        value.Message,
			Hint:           value.Hint,
			Note:           value.Note,
			SourceIdentity: value.SourceIdentity,
			Spans:          spans,
		}
	}

	return result
}

func plan(value core.PlanSnapshot) *wirev1.Plan {
	return &wirev1.Plan{
		Id:         &wirev1.PlanId{Value: string(value.ID)},
		Parameters: append([]string(nil), value.Parameters...),
		Debuggable: value.Debuggable,
	}
}

func output(value *core.Output) *wirev1.Output {
	if value == nil {
		return nil
	}

	return &wirev1.Output{ContentType: value.ContentType, Content: append([]byte(nil), value.Content...)}
}

func failure(value *core.Failure) *wirev1.Failure {
	if value == nil {
		return nil
	}

	return &wirev1.Failure{Category: errorCategory(value.Category), Message: value.Message, Diagnostics: diagnostics(value.Diagnostics)}
}

func execution(value core.ExecutionSnapshot) *wirev1.Execution {
	return &wirev1.Execution{
		Id:      &wirev1.ExecutionId{Value: string(value.ID)},
		PlanId:  &wirev1.PlanId{Value: string(value.PlanID)},
		State:   executionState(value.State),
		Output:  output(value.Output),
		Failure: failure(value.Failure),
	}
}

func executionState(value core.ExecutionState) wirev1.ExecutionState {
	switch value {
	case core.ExecutionRunning:
		return wirev1.ExecutionState_EXECUTION_STATE_RUNNING
	case core.ExecutionCompleted:
		return wirev1.ExecutionState_EXECUTION_STATE_COMPLETED
	case core.ExecutionFailed:
		return wirev1.ExecutionState_EXECUTION_STATE_FAILED
	case core.ExecutionCancelled:
		return wirev1.ExecutionState_EXECUTION_STATE_CANCELLED
	default:
		return wirev1.ExecutionState_EXECUTION_STATE_UNSPECIFIED
	}
}

func executionEvent(value core.ExecutionEvent) *wirev1.WatchExecutionResponse {
	result := &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: string(value.Execution)},
		Sequence:    value.Sequence,
	}
	snapshot := execution(value.Snapshot)
	switch value.Kind {
	case core.ExecutionEventStarted:
		result.Payload = &wirev1.WatchExecutionResponse_Started{Started: &wirev1.ExecutionStarted{Execution: snapshot}}
	case core.ExecutionEventCompleted:
		result.Payload = &wirev1.WatchExecutionResponse_Completed{Completed: &wirev1.ExecutionCompleted{Execution: snapshot}}
	case core.ExecutionEventFailed:
		result.Payload = &wirev1.WatchExecutionResponse_Failed{Failed: &wirev1.ExecutionFailed{Execution: snapshot}}
	case core.ExecutionEventCancelled:
		result.Payload = &wirev1.WatchExecutionResponse_Cancelled{Cancelled: &wirev1.ExecutionCancelled{Execution: snapshot}}
	}

	return result
}

func location(value core.Location) *wirev1.SourceLocation {
	if value == (core.Location{}) {
		return nil
	}

	return &wirev1.SourceLocation{File: value.File, Line: int32(value.Line), Column: int32(value.Column)}
}

func debugSession(value core.DebugSnapshot) *wirev1.DebugSession {
	return &wirev1.DebugSession{
		Id:               &wirev1.DebugSessionId{Value: string(value.ID)},
		PlanId:           &wirev1.PlanId{Value: string(value.PlanID)},
		State:            debugState(value.State),
		StopReason:       debugStopReason(value.StopReason),
		Location:         location(value.Location),
		HitBreakpointIds: append([]uint64(nil), value.HitBreakpointIDs...),
		Output:           output(value.Output),
		Failure:          failure(value.Failure),
	}
}

func debugState(value core.DebugState) wirev1.DebugState {
	switch value {
	case core.DebugCreated:
		return wirev1.DebugState_DEBUG_STATE_CREATED
	case core.DebugRunning:
		return wirev1.DebugState_DEBUG_STATE_RUNNING
	case core.DebugStopped:
		return wirev1.DebugState_DEBUG_STATE_STOPPED
	case core.DebugCompleted:
		return wirev1.DebugState_DEBUG_STATE_COMPLETED
	case core.DebugFailed:
		return wirev1.DebugState_DEBUG_STATE_FAILED
	case core.DebugTerminated:
		return wirev1.DebugState_DEBUG_STATE_TERMINATED
	default:
		return wirev1.DebugState_DEBUG_STATE_UNSPECIFIED
	}
}

func debugStopReason(value core.DebugStopReason) wirev1.DebugStopReason {
	switch value {
	case core.DebugStopEntry:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY
	case core.DebugStopBreakpoint:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT
	case core.DebugStopStep:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP
	case core.DebugStopPause:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE
	case core.DebugStopRuntimeError:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR
	default:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED
	}
}

func debugEvent(value core.DebugEvent) *wirev1.WatchDebugResponse {
	result := &wirev1.WatchDebugResponse{
		DebugSessionId: &wirev1.DebugSessionId{Value: string(value.Session)},
		Sequence:       value.Sequence,
	}
	snapshot := debugSession(value.Snapshot)
	switch value.Kind {
	case core.DebugEventStarted:
		result.Payload = &wirev1.WatchDebugResponse_Started{Started: &wirev1.DebugStarted{Session: snapshot}}
	case core.DebugEventContinued:
		result.Payload = &wirev1.WatchDebugResponse_Continued{Continued: &wirev1.DebugContinued{Session: snapshot}}
	case core.DebugEventStopped:
		result.Payload = &wirev1.WatchDebugResponse_Stopped{Stopped: &wirev1.DebugStopped{Session: snapshot}}
	case core.DebugEventCompleted:
		result.Payload = &wirev1.WatchDebugResponse_Completed{Completed: &wirev1.DebugCompleted{Session: snapshot}}
	case core.DebugEventFailed:
		result.Payload = &wirev1.WatchDebugResponse_Failed{Failed: &wirev1.DebugFailed{Session: snapshot}}
	case core.DebugEventTerminated:
		result.Payload = &wirev1.WatchDebugResponse_Terminated{Terminated: &wirev1.DebugTerminated{Session: snapshot}}
	}

	return result
}

func breakpoint(value core.Breakpoint) *wirev1.Breakpoint {
	return &wirev1.Breakpoint{
		Id:              value.ID,
		File:            value.File,
		RequestedLine:   int32(value.RequestedLine),
		RequestedColumn: int32(value.RequestedColumn),
		Line:            int32(value.Line),
		Column:          int32(value.Column),
		Verified:        value.Verified,
	}
}

func debugValue(value core.DebugValue) *wirev1.DebugValue {
	return &wirev1.DebugValue{Type: value.Type, Display: value.Display, Reference: value.Reference}
}

func variable(value core.Variable) *wirev1.Variable {
	return &wirev1.Variable{
		Name:      value.Name,
		Value:     debugValue(value.Value),
		Mutable:   value.Mutable,
		Parameter: value.Parameter,
	}
}
