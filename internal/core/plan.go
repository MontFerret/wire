package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	Plan struct {
		mu             sync.Mutex
		id             PlanID
		owner          ConnectionID
		plan           api.Plan
		parameters     []string
		debuggable     bool
		closing        bool
		childCreations sync.WaitGroup
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
	return PlanSnapshot{ID: p.id, Parameters: append([]string(nil), p.parameters...)}
}

func (p *Plan) beginChildCreation(debug bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closing {
		return notFound(ErrorPlanNotFound, string(p.id))
	}

	if debug && !p.debuggable {
		return invalidState("plan was not compiled for debugging", nil)
	}

	p.childCreations.Add(1)

	return nil
}

func (p *Plan) finishChildCreation() {
	p.childCreations.Done()
}

func (p *Plan) markClosing() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closing {
		return false
	}

	p.closing = true

	return p.release.Begin()
}

func (p *Plan) waitChildCreations() {
	p.childCreations.Wait()
}

func (p *Plan) finishClose(err error) {
	p.release.Finish(err)
}

func (p *Plan) waitClose(ctx context.Context) error {
	return p.release.Wait(ctx)
}
