package client

import wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"

func convertRuntimeInfo(value *wirev1.RuntimeInfo) RuntimeInfo {
	result := RuntimeInfo{
		APIIdentity:   value.GetApiIdentity(),
		WireVersion:   value.GetWireVersion(),
		FerretVersion: value.GetFerretVersion(),
	}

	if identity := value.GetRuntimeIdentity(); identity != nil {
		result.RuntimeIdentity = &RuntimeIdentity{
			Name:       identity.GetName(),
			Version:    identity.GetVersion(),
			InstanceID: identity.GetInstanceId(),
		}
	}

	for _, capability := range value.GetCapabilities() {
		switch capability {
		case wirev1.Capability_CAPABILITY_EXECUTION:
			result.Capabilities.Execution = true
		case wirev1.Capability_CAPABILITY_DEBUGGING:
			result.Capabilities.Debugging = true
		case wirev1.Capability_CAPABILITY_CANCELLATION:
			result.Capabilities.Cancellation = true
		}
	}

	return result
}

func convertOutput(value *wirev1.Output) *Output {
	if value == nil {
		return nil
	}

	return &Output{ContentType: value.GetContentType(), Content: append([]byte(nil), value.GetContent()...)}
}

func convertDiagnostics(values []*wirev1.Diagnostic) []Diagnostic {
	result := make([]Diagnostic, len(values))

	for i, value := range values {
		if value == nil {
			continue
		}

		spans := make([]DiagnosticSpan, len(value.GetSpans()))
		for j, span := range value.GetSpans() {
			if span == nil {
				continue
			}

			spans[j] = DiagnosticSpan{
				Start:   span.GetStartByte(),
				End:     span.GetEndByte(),
				Label:   span.GetLabel(),
				Primary: span.GetPrimary(),
			}
		}

		result[i] = Diagnostic{
			Kind:           value.GetKind(),
			Message:        value.GetMessage(),
			Hint:           value.GetHint(),
			Note:           value.GetNote(),
			SourceIdentity: value.GetSourceIdentity(),
			Spans:          spans,
		}
	}

	return result
}

func convertFailure(value *wirev1.Failure) *Failure {
	if value == nil {
		return nil
	}

	return &Failure{
		Category:    clientErrorCategory(value.GetCategory()),
		Message:     value.GetMessage(),
		Diagnostics: convertDiagnostics(value.GetDiagnostics()),
	}
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
		result.Kind = ExecutionEventStarted
		result.Snapshot = convertExecutionSnapshot(payload.Started.GetExecution())
	case *wirev1.WatchExecutionResponse_Completed:
		result.Kind = ExecutionEventCompleted
		result.Snapshot = convertExecutionSnapshot(payload.Completed.GetExecution())
	case *wirev1.WatchExecutionResponse_Failed:
		result.Kind = ExecutionEventFailed
		result.Snapshot = convertExecutionSnapshot(payload.Failed.GetExecution())
	case *wirev1.WatchExecutionResponse_Cancelled:
		result.Kind = ExecutionEventCancelled
		result.Snapshot = convertExecutionSnapshot(payload.Cancelled.GetExecution())
	}

	return result
}

func convertLocation(value *wirev1.SourceLocation) *Location {
	if value == nil {
		return nil
	}

	return &Location{File: value.GetFile(), Line: int(value.GetLine()), Column: int(value.GetColumn())}
}

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

func convertBreakpoint(value *wirev1.Breakpoint) Breakpoint {
	return Breakpoint{
		ID:              value.GetId(),
		File:            value.GetFile(),
		RequestedLine:   int(value.GetRequestedLine()),
		RequestedColumn: int(value.GetRequestedColumn()),
		Line:            int(value.GetLine()),
		Column:          int(value.GetColumn()),
		Verified:        value.GetVerified(),
	}
}

func convertDebugValue(value *wirev1.DebugValue) DebugValue {
	return DebugValue{Type: value.GetType(), Display: value.GetDisplay(), Reference: value.GetReference()}
}

func convertVariable(value *wirev1.Variable) Variable {
	return Variable{
		Name:      value.GetName(),
		Value:     convertDebugValue(value.GetValue()),
		Mutable:   value.GetMutable(),
		Parameter: value.GetParameter(),
	}
}
