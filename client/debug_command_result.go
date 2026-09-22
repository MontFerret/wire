package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

func convertCommandResult(value *wirev1.DebugCommandResult) (*wiredebugger.CommandResult, error) {
	if value == nil {
		return nil, nil
	}

	commandErr, err := convertCommandError(value.GetError(), value.GetContextFailure())
	if err != nil {
		return nil, err
	}

	result := &wiredebugger.CommandResult{Error: commandErr}

	if event := value.GetEvent(); event != nil {
		reason := debugger.Reason(event.GetReason())
		switch reason {
		case debugger.ReasonEntry, debugger.ReasonBreakpoint, debugger.ReasonStep, debugger.ReasonPause,
			debugger.ReasonRuntimeError, debugger.ReasonCompleted, debugger.ReasonTerminated:
		default:
			return nil, invalidDebuggerResponse("unknown command event reason")
		}

		location, err := convertSourceRange(event.GetLocation())
		if err != nil {
			return nil, err
		}

		depth, err := debuggerIntFromProto(event.GetDepth(), "debug depth", true)
		if err != nil {
			return nil, err
		}

		eventErr, err := convertCommandError(event.GetError(), event.GetContextFailure())
		if err != nil {
			return nil, err
		}

		var ids []debugger.BreakpointID

		if len(event.GetHitBreakpointIds()) > 0 {
			ids = make([]debugger.BreakpointID, len(event.GetHitBreakpointIds()))
		}

		for i, id := range event.GetHitBreakpointIds() {
			converted, err := debuggerIDFromProto[debugger.BreakpointID](id, "hit breakpoint ID", false)
			if err != nil {
				return nil, err
			}

			ids[i] = converted
		}

		result.Event = &debugger.Event{Reason: reason, Depth: depth, HitBreakpointIDs: ids, Output: convertOutput(event.GetOutput()), Error: eventErr}

		if location != nil {
			result.Event.Location = *location
		}
	}

	return result, nil
}

func convertCommandError(value *wirev1.Failure, cause wirev1.ContextFailure) (error, error) {
	failure, err := convertFailure(value)
	if err != nil {
		return nil, err
	}

	var result error

	if failure != nil {
		result = failure
	}

	switch cause {
	case wirev1.ContextFailure_CONTEXT_FAILURE_UNSPECIFIED:
	case wirev1.ContextFailure_CONTEXT_FAILURE_CANCELLED:
		result = errors.Join(result, context.Canceled)
	case wirev1.ContextFailure_CONTEXT_FAILURE_DEADLINE_EXCEEDED:
		result = errors.Join(result, context.DeadlineExceeded)
	default:
		return nil, invalidDebuggerResponse("unknown context failure")
	}

	return result, nil
}
