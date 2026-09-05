package server_test

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
)

type apiRuntimeSpy struct {
	mu         sync.Mutex
	compile    func(context.Context, api.Source, bool) (api.Plan, error)
	sources    []api.Source
	debug      []bool
	closeCalls int
}

func (r *apiRuntimeSpy) Run(context.Context, api.Source, ...api.SessionOption) (api.Output, error) {
	return api.Output{ContentType: "application/json", Content: []byte("1")}, nil
}

func (r *apiRuntimeSpy) Compile(ctx context.Context, src api.Source, _ ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, false)
}

func (r *apiRuntimeSpy) CompileDebug(ctx context.Context, src api.Source, _ ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, true)
}

func (r *apiRuntimeSpy) compilePlan(ctx context.Context, src api.Source, debug bool) (api.Plan, error) {
	r.mu.Lock()
	r.sources = append(r.sources, src)
	r.debug = append(r.debug, debug)
	compile := r.compile
	r.mu.Unlock()

	if compile == nil {
		return &apiPlanSpy{}, nil
	}

	return compile(ctx, src, debug)
}

func (r *apiRuntimeSpy) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()

	return nil
}
