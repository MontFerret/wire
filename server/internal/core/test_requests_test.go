package core

import (
	"github.com/MontFerret/api"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
)

type (
	compileRequest struct {
		Source               api.Source
		Debuggable           bool
		OptimizationLevel    api.OptimizationLevel
		HasOptimizationLevel bool
	}

	executeRequest struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	runRequest struct {
		Source            api.Source
		Parameters        map[string]any
		OutputContentType string
	}

	sessionRequest struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	debugRequest struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	planResult struct {
		ID         PlanID
		Parameters []string
	}

	executionResult struct {
		ID ExecutionID
		wireexecution.Snapshot
	}
	debugResult struct {
		ID DebugSessionID
		wiredebugger.Snapshot
	}
)

func apiSessionOptions(parameters map[string]any, contentType string) []api.SessionOption {
	options := []api.SessionOption{api.WithParams(cloneParameters(parameters))}

	if contentType != "" {
		options = append(options, api.WithOutputContentType(contentType))
	}

	return options
}
