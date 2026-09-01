package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
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
		Source     api.Source
		Debuggable bool
	}

	PlanSnapshot struct {
		ID         PlanID
		Parameters []string
		Debuggable bool
	}
)

func (c *Connection) Compile(ctx context.Context, input CompileInput) (PlanSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return PlanSnapshot{}, err
	}

	if input.Source.Content == "" {
		return PlanSnapshot{}, invalidRequest("source content is required")
	}

	if input.Source.Name == "" {
		input.Source.Name = "anonymous"
	}

	if err := c.beginPlanCreation(); err != nil {
		return PlanSnapshot{}, err
	}
	defer c.finishPlanCreation()

	compileCtx, cancel := c.operationContext(ctx)
	defer cancel()

	compiled, err, panicked := c.compileAPIPlan(compileCtx, input.Source, input.Debuggable)
	if panicked {
		return PlanSnapshot{}, internalError(err)
	}
	if err != nil {
		compileErr := &DomainError{
			Category: ErrorCompilation,
			Message:  "compilation failed",
			Cause:    err,
		}
		if !isNil(compiled) {
			return PlanSnapshot{}, errors.Join(compileErr, closeAPIPlan(compiled))
		}

		return PlanSnapshot{}, compileErr
	}

	if isNil(compiled) {
		return PlanSnapshot{}, internalError(errors.New("runtime returned no plan"))
	}

	if err := compileCtx.Err(); err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	parameters, err := apiPlanParameters(compiled)
	if err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	created := &Plan{
		id:         PlanID(uuid.NewString()),
		plan:       compiled,
		parameters: parameters,
		debuggable: input.Debuggable,
		executions: make(map[ExecutionID]struct{}),
		debug:      make(map[DebugSessionID]struct{}),
	}

	c.mu.Lock()
	if err := c.ensureOpenLocked(); err != nil {
		c.mu.Unlock()
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}
	c.plans[created.id] = created
	c.mu.Unlock()

	return created.snapshot(), nil
}

func (c *Connection) compileAPIPlan(ctx context.Context, src api.Source, debug bool) (compiled api.Plan, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			compiled = nil
			err = errors.New("runtime compilation panicked")
			panicked = true
		}
	}()

	if debug {
		compiled, err = c.runtime.CompileDebug(ctx, src)
	} else {
		compiled, err = c.runtime.Compile(ctx, src)
	}

	return
}

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
		Debuggable: p.debuggable,
	}
}

func (c *Connection) ReleasePlan(ctx context.Context, id PlanID) error {
	if err := validateID(id, "plan ID"); err != nil {
		return err
	}

	c.mu.Lock()
	plan := c.plans[id]
	if plan == nil {
		plan = c.closingPlans[id]
	}

	if plan == nil {
		c.mu.Unlock()
		return notFound(ErrorPlanNotFound, string(id))
	}

	if _, active := c.plans[id]; active {
		plan.mu.Lock()
		plan.closing = true
		plan.mu.Unlock()
		delete(c.plans, id)
		c.closingPlans[id] = plan
	}
	c.mu.Unlock()

	if plan.release.Begin() {
		go func() {
			var err error
			defer func() {
				if recover() != nil {
					err = errors.Join(err, internalError(errors.New("plan cleanup panicked")))
				}

				c.finishPlanRelease(plan)
				plan.release.Finish(err)
			}()

			err = c.settlePlanRelease(plan)
		}()
	}

	return plan.release.Wait(ctx)
}

func (c *Connection) finishPlanRelease(plan *Plan) {
	c.mu.Lock()
	if c.closingPlans[plan.id] == plan {
		delete(c.closingPlans, plan.id)
	}
	c.mu.Unlock()
}

func (c *Connection) settlePlanRelease(plan *Plan) error {
	plan.mu.Lock()
	executionIDs := make([]ExecutionID, 0, len(plan.executions))
	for child := range plan.executions {
		executionIDs = append(executionIDs, child)
	}
	debugIDs := make([]DebugSessionID, 0, len(plan.debug))
	for child := range plan.debug {
		debugIDs = append(debugIDs, child)
	}
	plan.mu.Unlock()

	var result error
	for _, child := range debugIDs {
		err := c.ReleaseDebugSession(context.Background(), child)
		result = errors.Join(result, ignoreMissingResource(err, ErrorDebugSessionNotFound))
	}

	for _, child := range executionIDs {
		err := c.ReleaseExecution(context.Background(), child)
		result = errors.Join(result, ignoreMissingResource(err, ErrorExecutionNotFound))
	}
	result = errors.Join(result, closeAPIPlan(plan.plan))
	return result
}
