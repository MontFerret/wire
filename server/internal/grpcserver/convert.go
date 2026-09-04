package grpcserver

import (
	"fmt"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	wirefailure "github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/core"
)

func protocolInfo(value core.RuntimeInfo) *wirev1.ProtocolInfo {
	return &wirev1.ProtocolInfo{Name: value.ProtocolName, Version: value.ProtocolVersion}
}

func runtimeIdentity(value wireexecution.Identity) *wirev1.RuntimeIdentity {
	if value == (wireexecution.Identity{}) {
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

func output(value *api.Output) *wirev1.Output {
	if value == nil {
		return nil
	}

	return &wirev1.Output{ContentType: value.ContentType, Content: append([]byte(nil), value.Content...)}
}

func failure(value *wirefailure.Failure) (*wirev1.Failure, error) {
	if value == nil {
		return nil, nil
	}

	diagnosticSet, err := diagnosticsToProto(value.Diagnostics)
	if err != nil {
		return nil, err
	}

	category, err := failureCategory(value.Category)
	if err != nil {
		return nil, err
	}

	return &wirev1.Failure{
		Category:      category,
		Message:       value.Message,
		DiagnosticSet: diagnosticSet,
	}, nil
}

func failureCategory(value wirefailure.Category) (wirev1.ErrorCategory, error) {
	switch value {
	case wirefailure.CategoryCompilation:
		return wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE, nil
	case wirefailure.CategoryExecution:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE, nil
	case wirefailure.CategoryPlanNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_PLAN_NOT_FOUND, nil
	case wirefailure.CategoryExecutionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_NOT_FOUND, nil
	case wirefailure.CategoryDebugSessionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_DEBUG_SESSION_NOT_FOUND, nil
	case wirefailure.CategoryConnectionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND, nil
	case wirefailure.CategoryInvalidState:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE, nil
	case wirefailure.CategoryInternalRuntime:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE, nil
	case wirefailure.CategoryWatcherLagged:
		return wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED, nil
	case wirefailure.CategoryBreakpointNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND, nil
	case wirefailure.CategorySessionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_SESSION_NOT_FOUND, nil
	}

	return 0, runtimeConversionError("runtime returned an invalid failure category")
}

func execution(value core.ExecutionRecord) (*wirev1.Execution, error) {
	state, err := executionState(value.Snapshot.State)
	if err != nil {
		return nil, err
	}

	convertedFailure, err := failure(value.Snapshot.Failure)
	if err != nil {
		return nil, err
	}

	return &wirev1.Execution{
		Id:      &wirev1.ExecutionId{Value: string(value.ID)},
		State:   state,
		Output:  output(value.Snapshot.Output),
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
	snapshot, err := execution(core.ExecutionRecord{ID: id, Snapshot: value.Snapshot})
	if err != nil {
		return nil, err
	}

	return &wirev1.WatchExecutionResponse{
		Sequence:  value.Sequence,
		Execution: snapshot,
	}, nil
}

func diagnosticsToProto(values diagnostics.Diagnostics) (*wirev1.DiagnosticSet, error) {
	if values == nil {
		return nil, nil
	}

	result := &wirev1.DiagnosticSet{Diagnostics: make([]*wirev1.Diagnostic, len(values))}
	for i, value := range values {
		annotations := make([]*wirev1.DiagnosticAnnotation, len(value.Annotations))
		for j, annotation := range value.Annotations {
			convertedRange, err := sourceRange(annotation.Range)
			if err != nil {
				return nil, err
			}

			if convertedRange == nil {
				return nil, runtimeConversionError("runtime returned a diagnostic annotation with no range")
			}

			annotations[j] = &wirev1.DiagnosticAnnotation{
				Range:   convertedRange,
				Message: annotation.Message,
				Primary: annotation.Primary,
			}
		}

		result.Diagnostics[i] = &wirev1.Diagnostic{
			Kind:        value.Kind.String(),
			Message:     value.Message,
			Hint:        value.Hint,
			Note:        value.Note,
			Source:      &wirev1.Source{Name: value.Source.Name, Content: value.Source.Content},
			Annotations: annotations,
		}
	}

	return result, nil
}

func sourceLocation(value source.Location) (*wirev1.Location, error) {
	if value == (source.Location{}) {
		return nil, nil
	}

	if value.SourceName == "" {
		return nil, runtimeConversionError("runtime returned a source location with no source name")
	}

	if value.Line <= 0 || value.Column < 0 {
		return nil, runtimeConversionError("runtime returned an invalid source location")
	}

	return &wirev1.Location{
		SourceName: value.SourceName,
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
		return source.Location{}, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " is required"}
	}

	if value.GetSourceName() == "" {
		return source.Location{}, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " source name is required"}
	}

	position := value.GetPosition()
	if position == nil || position.GetLine() <= 0 || position.GetColumn() < 0 {
		return source.Location{}, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " position is invalid"}
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
		SourceName: value.GetSourceName(),
		Position: source.Position{
			Line:   line,
			Column: column,
		},
	}, nil
}

func intFromProto(value int64, name string) (int, error) {
	if value < 0 || uint64(value) > uint64(^uint(0)>>1) {
		return 0, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " is out of range"}
	}

	return int(value), nil
}

func debugSession(value core.DebugSessionRecord) (*wirev1.DebugSession, error) {
	state, err := debugState(value.Snapshot.State)
	if err != nil {
		return nil, err
	}

	stopReason, err := debugStopReason(value.Snapshot.StopReason)
	if err != nil {
		return nil, err
	}

	var location *wirev1.Range
	if value.Snapshot.Location != nil {
		location, err = sourceRange(*value.Snapshot.Location)
	}
	if err != nil {
		return nil, err
	}
	if value.Snapshot.Location != nil && location == nil {
		return nil, runtimeConversionError("runtime returned an empty debug location")
	}

	if value.Snapshot.Depth < 0 {
		return nil, runtimeConversionError("runtime returned an invalid debug depth")
	}

	convertedFailure, err := failure(value.Snapshot.Failure)
	if err != nil {
		return nil, err
	}

	hitIDs := make([]uint64, len(value.Snapshot.HitBreakpointIDs))
	for i, id := range value.Snapshot.HitBreakpointIDs {
		converted, err := debuggerIDToProto(id, "hit breakpoint ID", false)
		if err != nil {
			return nil, err
		}

		hitIDs[i] = converted
	}

	return &wirev1.DebugSession{
		Id:               &wirev1.DebugSessionId{Value: string(value.ID)},
		State:            state,
		StopReason:       stopReason,
		Location:         location,
		HitBreakpointIds: hitIDs,
		Output:           output(value.Snapshot.Output),
		Failure:          convertedFailure,
		Depth:            int64(value.Snapshot.Depth),
	}, nil
}

func debugState(value wiredebugger.State) (wirev1.DebugState, error) {
	switch value {
	case wiredebugger.StateCreated:
		return wirev1.DebugState_DEBUG_STATE_CREATED, nil
	case wiredebugger.StateRunning:
		return wirev1.DebugState_DEBUG_STATE_RUNNING, nil
	case wiredebugger.StateStopped:
		return wirev1.DebugState_DEBUG_STATE_STOPPED, nil
	case wiredebugger.StateCompleted:
		return wirev1.DebugState_DEBUG_STATE_COMPLETED, nil
	case wiredebugger.StateFailed:
		return wirev1.DebugState_DEBUG_STATE_FAILED, nil
	case wiredebugger.StateTerminated:
		return wirev1.DebugState_DEBUG_STATE_TERMINATED, nil
	}

	return 0, runtimeConversionError("runtime returned an invalid debug state")
}

func debugStopReason(value debugger.Reason) (wirev1.DebugStopReason, error) {
	switch value {
	case "":
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED, nil
	case debugger.ReasonEntry:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY, nil
	case debugger.ReasonBreakpoint:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, nil
	case debugger.ReasonStep:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP, nil
	case debugger.ReasonPause:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, nil
	case debugger.ReasonRuntimeError:
		return wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR, nil
	}

	return 0, runtimeConversionError("runtime returned an invalid debug stop reason")
}

func debugEvent(id core.DebugSessionID, value wiredebugger.Event) (*wirev1.WatchDebugResponse, error) {
	kind, err := debugEventKind(value.Kind)
	if err != nil {
		return nil, err
	}

	snapshot, err := debugSession(core.DebugSessionRecord{ID: id, Snapshot: value.Snapshot})
	if err != nil {
		return nil, err
	}

	return &wirev1.WatchDebugResponse{
		Sequence: value.Sequence,
		Kind:     kind,
		Session:  snapshot,
	}, nil
}

func debugEventKind(value wiredebugger.EventKind) (wirev1.DebugEventKind, error) {
	switch value {
	case wiredebugger.EventStarted:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_STARTED, nil
	case wiredebugger.EventContinued:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_CONTINUED, nil
	case wiredebugger.EventStopped:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED, nil
	case wiredebugger.EventCompleted:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_COMPLETED, nil
	case wiredebugger.EventFailed:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_FAILED, nil
	case wiredebugger.EventTerminated:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_TERMINATED, nil
	case wiredebugger.EventCreated:
		return wirev1.DebugEventKind_DEBUG_EVENT_KIND_CREATED, nil
	}

	return 0, runtimeConversionError("runtime returned an invalid debug event kind")
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
	case debugger.BreakpointBindNextExecutableInSource:
		return wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_SOURCE, nil
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
		wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_SOURCE:
		return debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindNextExecutableInSource}, nil
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_EXACT:
		return debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindExact}, nil
	case wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FUNCTION:
		return debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindNextExecutableInFunction}, nil
	default:
		return debugger.BreakpointOptions{}, &core.DomainError{
			Kind:    core.ErrorKindInvalidRequest,
			Message: "breakpoint binding mode is invalid",
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
		return 0, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " must be positive"}
	}

	if value > uint64(^uint(0)>>1) {
		return 0, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: name + " is out of range"}
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
		Kind:    core.ErrorKindInternal,
		Message: "internal runtime failure",
		Cause:   fmt.Errorf(format, args...),
	}
}
