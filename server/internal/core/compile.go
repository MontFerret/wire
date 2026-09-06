package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

// CompilePlan creates a connection-owned plan using the borrowed hosted runtime.
func CompilePlan(ctx context.Context, runtime api.Runtime, store *ResourceStore, source api.Source, debug bool, options ...api.PlanOption) (*Plan, error) {
	if err := store.operationError(ctx); err != nil {
		return nil, err
	}

	if source.Content == "" {
		return nil, invalidRequest("source content is required")
	}

	if source.Name == "" {
		source.Name = "anonymous"
	}

	if err := store.beginCreation(planResource, nil); err != nil {
		return nil, err
	}

	committed := false
	defer func() { store.finishCreation(planResource, nil, committed) }()

	compile := runtime.Compile

	if debug {
		compile = runtime.CompileDebug
	}

	compiled, err := panicboundary.Call(func() (api.Plan, error) {
		return compile(ctx, source, options...)
	})
	if err != nil {
		var panicErr *panicboundary.Error
		if errors.As(err, &panicErr) {
			return nil, internalError(fmt.Errorf("compile runtime plan: %w", err))
		}

		compileErr := compilationError("compilation failed", err)

		if !isNil(compiled) {
			return nil, errors.Join(compileErr, closeAPIPlan(compiled))
		}

		return nil, compileErr
	}

	if isNil(compiled) {
		return nil, internalError(errors.New("runtime returned no plan"))
	}

	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, closeAPIPlan(compiled))
	}

	parameters, err := apiPlanParameters(compiled)
	if err != nil {
		return nil, errors.Join(err, closeAPIPlan(compiled))
	}

	created := &Plan{
		id:            PlanID(uuid.NewString()),
		store:         store,
		plan:          compiled,
		parameters:    parameters,
		debuggable:    debug,
		sessions:      make(map[SessionID]*Session),
		executions:    make(map[ExecutionID]*Execution),
		debugSessions: make(map[DebugSessionID]*DebugSession),
	}
	if err := store.registerPlan(ctx, created); err != nil {
		return nil, errors.Join(err, closeAPIPlan(compiled))
	}

	committed = true

	return created, nil
}
