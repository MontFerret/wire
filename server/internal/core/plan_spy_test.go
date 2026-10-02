package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

type spyPlan struct {
	mu              sync.Mutex
	params          []string
	paramsCall      func(context.Context) ([]string, error)
	paramsError     error
	newSession      func(context.Context, sessionOptions) (api.Session, error)
	newDebugSession func(context.Context, sessionOptions) (debugger.Session, error)
	sessionOptions  []sessionOptions
	debugOptions    []sessionOptions
	close           func() error
	closeCalls      int
}

func (p *spyPlan) Params(ctx context.Context) ([]string, error) {
	p.mu.Lock()
	call, params, err := p.paramsCall, p.params, p.paramsError
	p.mu.Unlock()

	if call != nil {
		return call(ctx)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	return params, err
}

func (p *spyPlan) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	configured, err := applySessionOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.sessionOptions = append(p.sessionOptions, configured.clone())
	newSession := p.newSession
	p.mu.Unlock()

	if newSession == nil {
		return &spySession{}, nil
	}

	return newSession(ctx, configured)
}

func (p *spyPlan) NewDebugSession(ctx context.Context, options ...api.SessionOption) (debugger.Session, error) {
	configured, err := applySessionOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.debugOptions = append(p.debugOptions, configured.clone())
	newDebugSession := p.newDebugSession
	p.mu.Unlock()

	if newDebugSession == nil {
		return &spyDebugger{}, nil
	}

	return newDebugSession(ctx, configured)
}

func (p *spyPlan) Close() error {
	p.mu.Lock()
	p.closeCalls++
	closePlan := p.close
	p.mu.Unlock()

	if closePlan == nil {
		return nil
	}

	return closePlan()
}

func (p *spyPlan) snapshot() ([]sessionOptions, []sessionOptions, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sessions := make([]sessionOptions, len(p.sessionOptions))
	for i, options := range p.sessionOptions {
		sessions[i] = options.clone()
	}

	debugSessions := make([]sessionOptions, len(p.debugOptions))
	for i, options := range p.debugOptions {
		debugSessions[i] = options.clone()
	}

	return sessions, debugSessions, p.closeCalls
}
