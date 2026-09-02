package core

import (
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	Plan struct {
		mu             sync.Mutex
		id             PlanID
		plan           api.Plan
		parameters     []string
		debuggable     bool
		closing        bool
		executions     map[ExecutionID]struct{}
		debugSessions  map[DebugSessionID]struct{}
		debugCreations sync.WaitGroup
		release        lifecycle.Close
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

func apiPlanParameters(plan api.Plan) (parameters []string, err error) {
	defer func() {
		if recover() != nil {
			parameters = nil
			err = internalError(errors.New("runtime plan metadata panicked"))
		}
	}()

	return append([]string(nil), plan.Params()...), nil
}

func closeAPIPlan(plan api.Plan) (err error) {
	defer func() {
		if recover() != nil {
			err = internalError(errors.New("runtime plan cleanup panicked"))
		}
	}()

	return plan.Close()
}

func (p *Plan) snapshot() PlanSnapshot {
	return PlanSnapshot{
		ID:         p.id,
		Parameters: append([]string(nil), p.parameters...),
	}
}
