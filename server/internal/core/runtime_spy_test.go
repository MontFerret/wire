package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
)

type spyRuntime struct {
	mu                  sync.Mutex
	run                 func(context.Context, api.Source, sessionOptions) (api.Output, error)
	compile             func(context.Context, api.Source, bool) (api.Plan, error)
	runSources          []api.Source
	runOptions          []sessionOptions
	compileSources      []api.Source
	compileDebug        []bool
	compileOptimization []planOptions
	closeCalls          int
}

func (r *spyRuntime) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (api.Output, error) {
	configured, err := applySessionOptions(options)
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

func (r *spyRuntime) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, false, options)
}

func (r *spyRuntime) CompileDebug(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, true, options)
}

func (r *spyRuntime) compilePlan(ctx context.Context, src api.Source, debug bool, options []api.PlanOption) (api.Plan, error) {
	configured := &planOptions{}
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
	r.compileOptimization = append(r.compileOptimization, *configured)
	compile := r.compile
	r.mu.Unlock()

	if compile == nil {
		return &spyPlan{}, nil
	}

	return compile(ctx, src, debug)
}

func (r *spyRuntime) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()

	return nil
}

func (r *spyRuntime) snapshot() ([]api.Source, []bool, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]api.Source(nil), r.compileSources...), append([]bool(nil), r.compileDebug...), r.closeCalls
}

func (r *spyRuntime) optimizationSnapshot() []planOptions {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]planOptions(nil), r.compileOptimization...)
}
