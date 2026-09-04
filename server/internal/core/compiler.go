package core

import (
	"errors"
	"fmt"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/panicboundary"
	"github.com/google/uuid"
)

// Compiler owns the Unified API compilation use case.
type Compiler struct {
	runtime api.Runtime
	plans   *PlanRegistry
}

func NewCompiler(runtime api.Runtime, plans *PlanRegistry) (*Compiler, error) {
	if isNil(runtime) {
		return nil, invalidRequest("runtime is required")
	}

	return &Compiler{runtime: runtime, plans: plans}, nil
}

func (c *Compiler) Compile(ctx *Context, input CompileInput) (PlanSnapshot, error) {
	connection := ctx.Connection()

	if err := connection.beginOperation(); err != nil {
		return PlanSnapshot{}, err
	}
	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return PlanSnapshot{}, err
	}

	if input.Source.Content == "" {
		return PlanSnapshot{}, invalidRequest("source content is required")
	}

	if input.Source.Name == "" {
		input.Source.Name = "anonymous"
	}

	owner := connection.ID()
	if err := c.plans.reserve(owner); err != nil {
		return PlanSnapshot{}, err
	}

	reserved := true
	defer func() {
		if reserved {
			c.plans.rollback(owner)
		}
	}()

	compiled, err := c.compileAPIPlan(ctx, input)
	if err != nil {
		var panicErr *panicboundary.Error
		if errors.As(err, &panicErr) {
			return PlanSnapshot{}, internalError(fmt.Errorf("compile runtime plan: %w", err))
		}

		compileErr := compilationError("compilation failed", err)
		if !isNil(compiled) {
			return PlanSnapshot{}, errors.Join(compileErr, closeAPIPlan(compiled))
		}

		return PlanSnapshot{}, compileErr
	}

	if isNil(compiled) {
		return PlanSnapshot{}, internalError(errors.New("runtime returned no plan"))
	}

	if err := ctx.Err(); err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	parameters, err := apiPlanParameters(compiled)
	if err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	created := &Plan{
		id:         PlanID(uuid.NewString()),
		owner:      owner,
		plan:       compiled,
		parameters: parameters,
		debuggable: input.Debuggable,
	}

	if err := ctx.Err(); err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	err = c.plans.commit(created)
	reserved = false
	if err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	return created.snapshot(), nil
}

func (c *Compiler) compileAPIPlan(ctx *Context, input CompileInput) (api.Plan, error) {
	var options []api.PlanOption
	if input.OptimizationLevel != nil {
		options = append(options, api.WithOptimizationLevel(*input.OptimizationLevel))
	}

	if input.Debuggable {
		return panicboundary.Call(func() (api.Plan, error) {
			return c.runtime.CompileDebug(ctx, input.Source, options...)
		})
	}

	return panicboundary.Call(func() (api.Plan, error) {
		return c.runtime.Compile(ctx, input.Source, options...)
	})
}
