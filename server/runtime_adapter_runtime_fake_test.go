package server_test

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
)

type contractRuntime struct {
	mu             sync.Mutex
	run            func(context.Context, api.Source, apiSessionOptions) (api.Output, error)
	compile        func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error)
	runSources     []api.Source
	runOptions     []apiSessionOptions
	compileSources []api.Source
	compileDebug   []bool
	compileLevels  []contractPlanOptions
	closeCalls     int
}

func (r *contractRuntime) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (api.Output, error) {
	configured, err := applyAPIOptions(options)
	if err != nil {
		return api.Output{}, err
	}

	r.mu.Lock()
	r.runSources = append(r.runSources, src)
	r.runOptions = append(r.runOptions, configured.clone())
	run := r.run
	r.mu.Unlock()

	if run == nil {
		return api.Output{}, nil
	}

	return run(ctx, src, configured)
}

func (r *contractRuntime) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, false, options)
}

func (r *contractRuntime) CompileDebug(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, true, options)
}

func (r *contractRuntime) compilePlan(
	ctx context.Context,
	src api.Source,
	debug bool,
	options []api.PlanOption,
) (api.Plan, error) {
	configured := &contractPlanOptions{}
	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(configured); err != nil {
			return nil, err
		}
	}

	r.mu.Lock()
	r.compileSources = append(r.compileSources, src)
	r.compileDebug = append(r.compileDebug, debug)
	r.compileLevels = append(r.compileLevels, *configured)
	compile := r.compile
	r.mu.Unlock()

	if compile == nil {
		return &contractPlan{}, nil
	}

	return compile(ctx, src, debug, *configured)
}

func (r *contractRuntime) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()

	return nil
}
