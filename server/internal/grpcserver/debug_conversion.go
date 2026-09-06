package grpcserver

import (
	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/server/internal/core"
)

func debugSession(id core.DebugSessionID, value wiredebugger.Snapshot) (*wirev1.DebugSession, error) {
	state, err := debugState(value.State)
	if err != nil {
		return nil, err
	}

	stopReason, err := debugStopReason(value.StopReason)
	if err != nil {
		return nil, err
	}

	var location *wirev1.Range

	if value.Location != nil {
		location, err = sourceRange(*value.Location)
		if err != nil {
			return nil, err
		}

		if location == nil {
			return nil, runtimeConversionError("runtime returned an empty debug location")
		}
	}

	if value.Depth < 0 {
		return nil, runtimeConversionError("runtime returned an invalid debug depth")
	}

	convertedFailure, err := failure(value.Failure)
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
		Id:               &wirev1.DebugSessionId{Value: string(id)},
		State:            state,
		StopReason:       stopReason,
		Location:         location,
		HitBreakpointIds: hitIDs,
		Output:           output(value.Output),
		Failure:          convertedFailure,
		Depth:            int64(value.Depth),
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

	snapshot, err := debugSession(id, value.Snapshot)
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

func variablesToProto(values []debugger.Variable) ([]*wirev1.Variable, error) {
	result := make([]*wirev1.Variable, len(values))
	for i, value := range values {
		converted, err := variable(value)
		if err != nil {
			return nil, err
		}

		result[i] = converted
	}

	return result, nil
}
