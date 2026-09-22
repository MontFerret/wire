package grpcserver

import (
	"context"
	"errors"

	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	wirefailure "github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/core"
)

func commandResult(value *wiredebugger.CommandResult) (*wirev1.DebugCommandResult, error) {
	if value == nil {
		return nil, nil
	}

	failure, err := commandError(value.Error, wirefailure.CategoryInternalRuntime)
	if err != nil {
		return nil, err
	}

	result := &wirev1.DebugCommandResult{Error: failure, ContextFailure: contextFailure(value.Error)}
	if value.Event != nil {
		event := value.Event
		switch event.Reason {
		case debugger.ReasonEntry, debugger.ReasonBreakpoint, debugger.ReasonStep, debugger.ReasonPause,
			debugger.ReasonRuntimeError, debugger.ReasonCompleted, debugger.ReasonTerminated:
		default:
			return nil, runtimeConversionError("runtime returned an invalid debugger reason")
		}

		location, err := sourceRange(event.Location)
		if err != nil {
			return nil, err
		}

		if event.Depth < 0 {
			return nil, runtimeConversionError("runtime returned an invalid debug depth")
		}

		failure, err := commandError(event.Error, wirefailure.CategoryExecution)
		if err != nil {
			return nil, err
		}

		ids := make([]uint64, len(event.HitBreakpointIDs))
		for i, id := range event.HitBreakpointIDs {
			converted, err := debuggerIDToProto(id, "hit breakpoint ID", false)
			if err != nil {
				return nil, err
			}

			ids[i] = converted
		}

		result.Event = &wirev1.DebugResultEvent{Reason: string(event.Reason), Location: location, Depth: int64(event.Depth),
			HitBreakpointIds: ids, Output: output(event.Output), Error: failure, ContextFailure: contextFailure(event.Error)}
	}

	return result, nil
}

func commandError(err error, category wirefailure.Category) (*wirev1.Failure, error) {
	//nolint:errorlint // Only bare context errors bypass sanitization; joined failures retain details.
	if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
		return nil, nil
	}

	var existing *wirefailure.Failure
	if errors.As(err, &existing) {
		return failure(existing)
	}

	return failure(&wirefailure.Failure{Category: category, Message: "runtime operation failed", Diagnostics: core.DiagnosticsFromError(err)})
}

func contextFailure(err error) wirev1.ContextFailure {
	if errors.Is(err, context.Canceled) {
		return wirev1.ContextFailure_CONTEXT_FAILURE_CANCELLED
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return wirev1.ContextFailure_CONTEXT_FAILURE_DEADLINE_EXCEEDED
	}

	return wirev1.ContextFailure_CONTEXT_FAILURE_UNSPECIFIED
}
