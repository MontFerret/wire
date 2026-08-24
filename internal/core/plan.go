package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/ferret/v2"
	"github.com/MontFerret/ferret/v2/pkg/source"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

type (
	Plan struct {
		mu         sync.Mutex
		id         PlanID
		plan       *ferret.Plan
		identity   string
		parameters []string
		debuggable bool
		closing    bool
		executions map[ExecutionID]struct{}
		debug      map[DebugSessionID]struct{}
		release    lifecycle.Close
	}

	CompileInput struct {
		Content    string
		Identity   string
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

	if input.Content == "" {
		return PlanSnapshot{}, invalidRequest("source content is required")
	}

	if input.Identity == "" {
		input.Identity = "anonymous"
	}

	if err := c.beginPlanCreation(); err != nil {
		return PlanSnapshot{}, err
	}
	defer c.finishPlanCreation()

	compileCtx, cancel := c.operationContext(ctx)
	defer cancel()

	src := source.New(input.Identity, input.Content)
	var compiled *ferret.Plan
	var err error
	if input.Debuggable {
		compiled, err = c.engine.CompileDebug(compileCtx, src)
	} else {
		compiled, err = c.engine.Compile(compileCtx, src)
	}

	if err != nil {
		return PlanSnapshot{}, &DomainError{
			Category:    ErrorCompilation,
			Message:     "compilation failed",
			Diagnostics: diagnosticsFromError(err, input.Identity),
			Cause:       err,
		}
	}

	if err := compileCtx.Err(); err != nil {
		return PlanSnapshot{}, errors.Join(err, compiled.Close())
	}

	created := &Plan{
		id:         PlanID(uuid.NewString()),
		plan:       compiled,
		identity:   input.Identity,
		parameters: compiled.Params(),
		debuggable: input.Debuggable,
		executions: make(map[ExecutionID]struct{}),
		debug:      make(map[DebugSessionID]struct{}),
	}

	c.mu.Lock()
	if err := c.ensureOpenLocked(); err != nil {
		c.mu.Unlock()
		return PlanSnapshot{}, errors.Join(err, compiled.Close())
	}

	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return PlanSnapshot{}, errors.Join(err, compiled.Close())
	}
	c.plans[created.id] = created
	c.mu.Unlock()

	return created.snapshot(), nil
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
	result = errors.Join(result, plan.plan.Close())
	return result
}
