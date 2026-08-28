package grpcserver

import (
	"fmt"
	"math"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
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

func sourceLocation(value source.Location) (*wirev1.SourceLocation, error) {
	if value == (source.Location{}) {
		return nil, nil
	}

	if value.Line <= 0 || value.Line > math.MaxInt32 || value.Column < 0 || value.Column > math.MaxInt32 {
		return nil, runtimeConversionError("runtime returned an invalid source location")
	}

	return &wirev1.SourceLocation{File: value.File, Line: int32(value.Line), Column: int32(value.Column)}, nil
}

func sourceRange(value source.Range) (*wirev1.SourceLocation, error) {
	return sourceLocation(value.Location)
}

func debugSession(value core.DebugSnapshot) (*wirev1.DebugSession, error) {
	location, err := sourceRange(value.Location)
	if err != nil {
		return nil, err
	}

	hitIDs := make([]uint64, len(value.HitBreakpointIDs))
	for i, id := range value.HitBreakpointIDs {
		converted, err := debuggerIDToProto(id, "hit breakpoint ID", false)
		if err != nil {
			return nil, err
		}

		hitIDs[i] = converted
	}

	return &wirev1.DebugSession{
		Id:               &wirev1.DebugSessionId{Value: string(value.ID)},
		PlanId:           &wirev1.PlanId{Value: string(value.PlanID)},
		State:            debugState(value.State),
		StopReason:       debugStopReason(value.StopReason),
		Location:         location,
		HitBreakpointIds: hitIDs,
		Output:           output(value.Output),
		Failure:          failure(value.Failure),
	}, nil
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

func debugStopReason(value debugger.Reason) wirev1.DebugStopReason {
	switch value {
	case debugger.ReasonEntry:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY
	case debugger.ReasonBreakpoint:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT
	case debugger.ReasonStep:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP
	case debugger.ReasonPause:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE
	case debugger.ReasonRuntimeError:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR
	default:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED
	}
}

func debugEvent(value core.DebugEvent) (*wirev1.WatchDebugResponse, error) {
	result := &wirev1.WatchDebugResponse{
		DebugSessionId: &wirev1.DebugSessionId{Value: string(value.Session)},
		Sequence:       value.Sequence,
	}
	snapshot, err := debugSession(value.Snapshot)
	if err != nil {
		return nil, err
	}

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

	return result, nil
}

func breakpoint(value debugger.Breakpoint) (*wirev1.Breakpoint, error) {
	if value.PointID < 0 || value.FunctionID < 0 {
		return nil, runtimeConversionError("runtime returned invalid breakpoint metadata")
	}

	id, err := debuggerIDToProto(value.ID, "breakpoint ID", false)
	if err != nil {
		return nil, err
	}

	requested, err := sourceLocation(value.RequestedLocation)
	if err != nil {
		return nil, err
	}

	if requested == nil {
		return nil, runtimeConversionError("runtime returned no requested breakpoint location")
	}

	if requested.File == "" {
		return nil, runtimeConversionError("runtime returned a breakpoint with no source file")
	}

	resolved, err := sourceRange(value.Location)
	if err != nil {
		return nil, err
	}

	if value.Bound && resolved == nil {
		return nil, runtimeConversionError("runtime returned a bound breakpoint with no resolved location")
	}

	if resolved != nil && resolved.File != requested.File {
		return nil, runtimeConversionError("runtime returned breakpoint locations in different files")
	}

	result := &wirev1.Breakpoint{
		Id:              id,
		File:            requested.File,
		RequestedLine:   requested.Line,
		RequestedColumn: requested.Column,
		Verified:        value.Bound,
	}
	if resolved != nil {
		result.Line = resolved.Line
		result.Column = resolved.Column
	}

	return result, nil
}

func frame(value debugger.Frame, index int) (*wirev1.Frame, error) {
	if index < 0 || index > math.MaxInt32 {
		return nil, runtimeConversionError("runtime returned too many frames")
	}

	if value.FunctionID < 0 {
		return nil, runtimeConversionError("runtime returned an invalid frame function ID")
	}

	location, err := sourceLocation(value.Location)
	if err != nil {
		return nil, err
	}

	return &wirev1.Frame{Index: int32(index), Name: value.Name, Location: location}, nil
}

func debugValue(value debugger.Value) (*wirev1.DebugValue, error) {
	reference, err := debuggerIDToProto(value.Reference, "debug value reference", true)
	if err != nil {
		return nil, err
	}

	return &wirev1.DebugValue{Type: value.Type, Display: value.Display, Reference: reference}, nil
}

func variable(value debugger.Variable) (*wirev1.Variable, error) {
	converted, err := debugValue(value.Value)
	if err != nil {
		return nil, err
	}

	return &wirev1.Variable{
		Name:      value.Name,
		Value:     converted,
		Mutable:   value.Mutable,
		Parameter: value.Param,
	}, nil
}

func debuggerIDFromProto[T ~int](value uint64, name string) (T, error) {
	if value == 0 {
		return 0, &core.DomainError{Category: core.ErrorInvalidRequest, Message: name + " must be positive"}
	}

	if value > uint64(^uint(0)>>1) {
		return 0, &core.DomainError{Category: core.ErrorInvalidRequest, Message: name + " is out of range"}
	}

	return T(value), nil
}

func debuggerIDToProto[T ~int](value T, name string, zeroAllowed bool) (uint64, error) {
	if value < 0 || (!zeroAllowed && value == 0) {
		return 0, runtimeConversionError("runtime returned an invalid %s", name)
	}

	return uint64(value), nil
}

func runtimeConversionError(format string, args ...any) error {
	return &core.DomainError{
		Category: core.ErrorInternal,
		Message:  "internal runtime failure",
		Cause:    fmt.Errorf(format, args...),
	}
}
