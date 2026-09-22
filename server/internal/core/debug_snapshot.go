package core

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
)

func cloneDebugSnapshot(snapshot wiredebugger.Snapshot) wiredebugger.Snapshot {
	result := snapshot
	result.HitBreakpointIDs = append(result.HitBreakpointIDs[:0:0], snapshot.HitBreakpointIDs...)

	if snapshot.Location != nil {
		location := *snapshot.Location
		result.Location = &location
	}

	if snapshot.Output != nil {
		result.Output = &api.Output{
			ContentType: snapshot.Output.ContentType,
			Content:     append([]byte(nil), snapshot.Output.Content...),
		}
	}

	if snapshot.Failure != nil {
		result.Failure = &failure.Failure{
			Category:    snapshot.Failure.Category,
			Message:     snapshot.Failure.Message,
			Diagnostics: cloneDiagnostics(snapshot.Failure.Diagnostics),
		}
	}

	result.CommandResult = cloneCommandResult(snapshot.CommandResult)

	return result
}

func cloneCommandResult(result *wiredebugger.CommandResult) *wiredebugger.CommandResult {
	if result == nil {
		return nil
	}

	cloned := &wiredebugger.CommandResult{Error: portableDebugError(result.Error, failure.CategoryInternalRuntime)}
	if result.Event != nil {
		event := *result.Event
		event.Output = cloneOutput(event.Output)
		event.HitBreakpointIDs = append(event.HitBreakpointIDs[:0:0], event.HitBreakpointIDs...)
		event.Error = portableDebugError(event.Error, failure.CategoryExecution)
		cloned.Event = &event
	}

	return cloned
}

func portableDebugError(err error, category failure.Category) error {
	//nolint:errorlint // Only bare context errors bypass sanitization; joined failures retain details.
	if err == nil || err == context.Canceled || err == context.DeadlineExceeded {
		return err
	}

	value := failureFromError(category, err)

	var existing *failure.Failure
	if errors.As(err, &existing) && existing != nil {
		value.Diagnostics = cloneDiagnostics(existing.Diagnostics)
	}

	if errors.Is(err, context.Canceled) {
		return errors.Join(value, context.Canceled)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(value, context.DeadlineExceeded)
	}

	return value
}
