package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/server/internal/lifecycle"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

type Plan struct {
	id         PlanID
	store      *ResourceStore
	plan       api.Plan
	parameters []string
	debuggable bool
	// Child collections and creation admission are guarded by store.mu.
	creating      sync.WaitGroup
	sessions      map[SessionID]*Session
	executions    map[ExecutionID]*Execution
	debugSessions map[DebugSessionID]*DebugSession
	release       lifecycle.Close
}

func (p *Plan) ID() PlanID {
	return p.id
}

func (p *Plan) Params() []string {
	return append([]string(nil), p.parameters...)
}

func (p *Plan) NewSession(ctx context.Context, options ...api.SessionOption) (*Session, error) {
	if err := p.store.operationError(ctx); err != nil {
		return nil, err
	}

	if err := p.store.beginCreation(sessionResource, p); err != nil {
		return nil, err
	}

	committed := false
	defer func() { p.store.finishCreation(sessionResource, p, committed) }()

	hosted, err := panicboundary.Call(func() (api.Session, error) {
		return p.plan.NewSession(ctx, options...)
	})
	if err != nil {
		var closeErr error
		if !isNil(hosted) {
			closeErr = closeAPISession(hosted)
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Join(ctxErr, closeErr)
		}

		return nil, errors.Join(internalError(err), closeErr)
	}

	if isNil(hosted) {
		return nil, internalError(errors.New("runtime returned no session"))
	}

	created := newSession(p, hosted)
	if err := p.store.registerSession(ctx, created); err != nil {
		created.cancel(context.Canceled)

		return nil, errors.Join(err, closeAPISession(hosted))
	}

	committed = true

	return created, nil
}

func (p *Plan) Execute(ctx context.Context, options ...api.SessionOption) (*Execution, error) {
	if err := p.store.operationError(ctx); err != nil {
		return nil, err
	}

	if err := p.store.beginCreation(executionResource, p); err != nil {
		return nil, err
	}

	committed := false
	defer func() { p.store.finishCreation(executionResource, p, committed) }()

	created := newExecution(p.store, p, nil, nil, options)
	if err := p.store.registerExecution(ctx, created); err != nil {
		created.cancel(context.Canceled)

		return nil, err
	}

	committed = true
	go created.run()

	return created, nil
}

func (p *Plan) NewDebugSession(ctx context.Context, options ...api.SessionOption) (*DebugSession, error) {
	if err := p.store.operationError(ctx); err != nil {
		return nil, err
	}

	if err := p.store.beginCreation(debugResource, p); err != nil {
		return nil, err
	}

	committed := false
	defer func() { p.store.finishCreation(debugResource, p, committed) }()

	if !p.debuggable {
		return nil, invalidState("plan was not compiled for debugging", nil)
	}

	hosted, err := panicboundary.Call(func() (debugger.Session, error) {
		return p.plan.NewDebugSession(ctx, options...)
	})
	if err != nil {
		var closeErr error
		if !isNil(hosted) {
			closeErr = closeAPIDebugSession(hosted)
		}

		return nil, errors.Join(internalError(err), closeErr)
	}

	if isNil(hosted) {
		return nil, internalError(errors.New("runtime returned no debug session"))
	}

	created := newDebugSession(p, hosted)
	if err := p.store.registerDebugSession(ctx, created); err != nil {
		return nil, errors.Join(err, created.Close(context.Background()))
	}

	committed = true

	return created, nil
}

func (p *Plan) Release(ctx context.Context) error {
	p.store.mu.Lock()
	started := p.release.Begin()
	p.store.mu.Unlock()
	if started {
		go p.settleRelease()
	}

	return p.release.Wait(ctx)
}

func (p *Plan) settleRelease() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("plan cleanup panicked")))
		}

		p.store.removePlan(p)

		p.release.Finish(err)
	}()

	p.creating.Wait()
	p.store.mu.Lock()
	executions := make([]*Execution, 0, len(p.executions))
	for _, execution := range p.executions {
		executions = append(executions, execution)
	}

	sessions := make([]*Session, 0, len(p.sessions))
	for _, session := range p.sessions {
		sessions = append(sessions, session)
	}

	debugSessions := make([]*DebugSession, 0, len(p.debugSessions))
	for _, session := range p.debugSessions {
		debugSessions = append(debugSessions, session)
	}

	p.store.mu.Unlock()
	for _, execution := range executions {
		err = errors.Join(err, execution.Release(context.Background()))
	}

	for _, session := range sessions {
		err = errors.Join(err, session.Release(context.Background()))
	}

	for _, session := range debugSessions {
		err = errors.Join(err, session.Release(context.Background()))
	}

	err = errors.Join(err, closeAPIPlan(p.plan))
}
