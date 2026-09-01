package core

import (
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	Plan struct {
		mu         sync.Mutex
		id         PlanID
		plan       api.Plan
		parameters []string
		debuggable bool
		closing    bool
		executions map[ExecutionID]struct{}
		debug      map[DebugSessionID]struct{}
		release    lifecycle.Close
	}

	CompileInput struct {
		Source            api.Source
		Debuggable        bool
		OptimizationLevel *api.OptimizationLevel
	}

	PlanSnapshot struct {
		ID         PlanID
		Parameters []string
	}
)

func (p *Plan) snapshot() PlanSnapshot {
	return PlanSnapshot{
		ID:         p.id,
		Parameters: append([]string(nil), p.parameters...),
	}
}
