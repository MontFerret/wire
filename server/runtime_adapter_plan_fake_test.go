package server_test

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

type contractPlan struct {
	mu              sync.Mutex
	params          []string
	newSession      func(context.Context, apiSessionOptions) (api.Session, error)
	newDebugSession func(context.Context, apiSessionOptions) (debugger.Session, error)
	sessionOptions  []apiSessionOptions
	debugOptions    []apiSessionOptions
	closeCalls      int
}

func (p *contractPlan) Params() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.params
}

func (p *contractPlan) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	configured, err := applyAPIOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.sessionOptions = append(p.sessionOptions, configured.clone())
	create := p.newSession
	p.mu.Unlock()
	if create == nil {
		return &contractSession{}, nil
	}

	return create(ctx, configured)
}

func (p *contractPlan) NewDebugSession(ctx context.Context, options ...api.SessionOption) (debugger.Session, error) {
	configured, err := applyAPIOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.debugOptions = append(p.debugOptions, configured.clone())
	create := p.newDebugSession
	p.mu.Unlock()
	if create == nil {
		return nil, errors.New("debug session is not configured")
	}

	return create(ctx, configured)
}

func (p *contractPlan) Close() error {
	p.mu.Lock()
	p.closeCalls++
	p.mu.Unlock()

	return nil
}
