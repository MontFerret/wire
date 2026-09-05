package server_test

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

type apiPlanSpy struct {
	mu             sync.Mutex
	params         []string
	newSession     func(context.Context, apiSessionOptions) (api.Session, error)
	sessionOptions []apiSessionOptions
	closeCalls     int
}

func (p *apiPlanSpy) Params() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.params...)
}

func (p *apiPlanSpy) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	configured := apiSessionOptions{params: make(map[string]any)}
	for _, option := range options {
		if err := option(&configured); err != nil {
			return nil, err
		}
	}

	p.mu.Lock()
	p.sessionOptions = append(p.sessionOptions, configured.clone())
	newSession := p.newSession
	p.mu.Unlock()

	if newSession == nil {
		return &apiSessionSpy{}, nil
	}

	return newSession(ctx, configured)
}

func (p *apiPlanSpy) NewDebugSession(context.Context, ...api.SessionOption) (debugger.Session, error) {
	return nil, errors.New("debug session is not configured")
}

func (p *apiPlanSpy) Close() error {
	p.mu.Lock()
	p.closeCalls++
	p.mu.Unlock()

	return nil
}
