package grpcserver

import (
	"fmt"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

func protocolInfo(value core.RuntimeInfo) *wirev1.ProtocolInfo {
	return &wirev1.ProtocolInfo{Name: value.ProtocolName, Version: value.ProtocolVersion}
}

func runtimeIdentity(value core.RuntimeIdentity) *wirev1.RuntimeIdentity {
	if value == (core.RuntimeIdentity{}) {
		return nil
	}

	return &wirev1.RuntimeIdentity{
		Name:       value.Name,
		Version:    value.Version,
		InstanceId: value.InstanceID,
	}
}

func plan(value core.PlanSnapshot) *wirev1.Plan {
	return &wirev1.Plan{
		Id:         &wirev1.PlanId{Value: string(value.ID)},
		Parameters: append([]string(nil), value.Parameters...),
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

	return &wirev1.Failure{Category: errorCategory(value.Category), Message: value.Message}
}

func execution(value core.ExecutionSnapshot) *wirev1.Execution {
	return &wirev1.Execution{
		Id:      &wirev1.ExecutionId{Value: string(value.ID)},
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
	return &wirev1.WatchExecutionResponse{
		Sequence:  value.Sequence,
		Execution: execution(value.Snapshot),
	}
}

func sourceLocation(value source.Location) (*wirev1.Location, error) {
	if value == (source.Location{}) {
		return nil, nil
	}

	if value.File == "" {
		return nil, runtimeConversionError("runtime returned a source location with no file")
	}

	if value.Line <= 0 || value.Column < 0 {
		return nil, runtimeConversionError("runtime returned an invalid source location")
	}

	return &wirev1.Location{
		File: value.File,
		Position: &wirev1.Position{
			Line:   int64(value.Line),
			Column: int64(value.Column),
		},
	}, nil
}

func sourceRange(value source.Range) (*wirev1.Range, error) {
	if value == (source.Range{}) {
		return nil, nil
	}

	location, err := sourceLocation(value.Location)
	if err != nil {
		return nil, err
	}

	if location == nil {
		return nil, runtimeConversionError("runtime returned a source range with no location")
	}

	if value.Span.Start < 0 || value.Span.End < value.Span.Start {
		return nil, runtimeConversionError("runtime returned an invalid source span")
	}

	return &wirev1.Range{
		Location: location,
		Span: &wirev1.Span{
			Start: int64(value.Span.Start),
			End:   int64(value.Span.End),
		},
	}, nil
}

func sourceLocationFromProto(value *wirev1.Location, name string) (source.Location, error) {
	if value == nil {
		return source.Location{}, &core.DomainError{Category: core.ErrorInvalidRequest, Message: name + " is required"}
	}

	if value.GetFile() == "" {
		return source.Location{}, &core.DomainError{Category: core.ErrorInvalidRequest, Message: name + " file is required"}
	}

	position := value.GetPosition()
	if position == nil || position.GetLine() <= 0 || position.GetColumn() < 0 {
		return source.Location{}, &core.DomainError{Category: core.ErrorInvalidRequest, Message: name + " position is invalid"}
	}

	line, err := intFromProto(position.GetLine(), name+" line")
	if err != nil {
		return source.Location{}, err
	}

	column, err := intFromProto(position.GetColumn(), name+" column")
	if err != nil {
		return source.Location{}, err
	}

	return source.Location{
		File: value.GetFile(),
		Position: source.Position{
			Line:   line,
			Column: column,
		},
	}, nil
}

func intFromProto(value int64, name string) (int, error) {
	if value < 0 || uint64(value) > uint64(^uint(0)>>1) {
		return 0, &core.DomainError{Category: core.ErrorInvalidRequest, Message: name + " is out of range"}
	}

	return int(value), nil
}

func debugSession(value core.DebugSnapshot) (*wirev1.DebugSession, error) {
	location, err := sourceRange(value.Location)
	if err != nil {
		return nil, err
	}

	if value.Depth < 0 {
		return nil, runtimeConversionError("runtime returned an invalid debug depth")
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
		State:            debugState(value.State),
		StopReason:       debugStopReason(value.StopReason),
		Location:         location,
		HitBreakpointIds: hitIDs,
		Output:           output(value.Output),
		Failure:          failure(value.Failure),
		Depth:            int64(value.Depth),
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
	snapshot, err := debugSession(value.Snapshot)
	if err != nil {
		return nil, err
	}

	return &wirev1.WatchDebugResponse{
		Sequence: value.Sequence,
		Kind:     debugEventKind(value.Kind),
		Session:  snapshot,
	}, nil
}

func debugEventKind(value core.DebugEventKind) wirev1.DebugEventKind {
	switch value {
	case core.DebugEventStarted:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_STARTED
	case core.DebugEventContinued:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_CONTINUED
	case core.DebugEventStopped:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED
	case core.DebugEventCompleted:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_COMPLETED
	case core.DebugEventFailed:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_FAILED
	case core.DebugEventTerminated:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_TERMINATED
	default:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_UNSPECIFIED
	}
}

func breakpoint(value debugger.Breakpoint) (*wirev1.Breakpoint, error) {
	id, err := debuggerIDToProto(value.ID, "breakpoint ID", false)
	if err != nil {
		return nil, err
	}

	pointID, err := debuggerIDToProto(value.PointID, "breakpoint point ID", true)
	if err != nil {
		return nil, err
	}

	functionID, err := debuggerIDToProto(value.FunctionID, "breakpoint function ID", true)
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

	resolved, err := sourceRange(value.Location)
	if err != nil {
		return nil, err
	}

	if value.Bound && resolved == nil {
		return nil, runtimeConversionError("runtime returned a bound breakpoint with no resolved location")
	}

	bindingMode, err := breakpointBindingMode(value.BindingMode)
	if err != nil {
		return nil, err
	}

	return &wirev1.Breakpoint{
		Id:                id,
		RequestedLocation: requested,
		Location:          resolved,
		PointId:           pointID,
		FunctionId:        functionID,
		BindingMode:       bindingMode,
		Bound:             value.Bound,
	}, nil
}

func breakpointBindingMode(value debugger.BreakpointBindingMode) (wirev1.BreakpointBindingMode, error) {
	switch value {
	case debugger.BreakpointBindNextExecutableInFile:
		return wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FILE, nil
	case debugger.BreakpointBindExact:
		return wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_EXACT, nil
	case debugger.BreakpointBindNextExecutableInFunction:
		return wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FUNCTION, nil
	default:
		return wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_UNSPECIFIED,
			runtimeConversionError("runtime returned an invalid breakpoint binding mode")
	}
}

func breakpointOptions(value *wirev1.BreakpointOptions) (debugger.BreakpointOptions, error) {
	mode := wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_UNSPECIFIED
	if value != nil {
		mode = value.GetBindingMode()
	}

	switch mode {
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_UNSPECIFIED,
		wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FILE:
		return debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindNextExecutableInFile}, nil
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_EXACT:
		return debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindExact}, nil
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FUNCTION:
		return debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindNextExecutableInFunction}, nil
	default:
		return debugger.BreakpointOptions{}, &core.DomainError{
			Category: core.ErrorInvalidRequest,
			Message:  "breakpoint binding mode is invalid",
		}
	}
}

func frame(value debugger.Frame) (*wirev1.Frame, error) {
	functionID, err := debuggerIDToProto(value.FunctionID, "frame function ID", true)
	if err != nil {
		return nil, err
	}

	location, err := sourceLocation(value.Location)
	if err != nil {
		return nil, err
	}

	return &wirev1.Frame{Name: value.Name, Location: location, FunctionId: functionID}, nil
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
