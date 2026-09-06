package harness

import (
	"context"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

type (
	// PlanBehavior configures child creation, option observation, and plan cleanup hooks.
	PlanBehavior struct {
		Params          []string
		NewSession      func(context.Context, SessionOptions) error
		NewDebugSession func(context.Context, SessionOptions) error
		Session         func(SessionOptions) SessionBehavior
		Debugger        DebuggerBehavior
		Close           func() error
	}

	// PlanSpy records hosted plan calls and creates observable child resources.
	PlanSpy struct {
		id       int
		recorder *Recorder
		behavior PlanBehavior
	}
)

var _ api.Plan = (*PlanSpy)(nil)

// Params records inspection and returns a copy of the configured parameter names.
func (p *PlanSpy) Params() []string {
	p.recorder.record(Call{Resource: p.id, Method: "Params"})

	return append([]string(nil), p.behavior.Params...)
}

// NewSession records applied options and creates a child spy after the creation hook succeeds.
func (p *PlanSpy) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	configured, err := applyOptions(options)
	if err != nil {
		return nil, err
	}

	p.recorder.record(Call{Resource: p.id, Method: "NewSession", Options: configured})

	if p.behavior.NewSession != nil {
		if err := p.behavior.NewSession(ctx, configured); err != nil {
			return nil, err
		}
	}

	var behavior SessionBehavior
	if p.behavior.Session != nil {
		behavior = p.behavior.Session(configured)
	}

	return &SessionSpy{id: p.recorder.create("session", p.id), recorder: p.recorder, behavior: behavior}, nil
}

// NewDebugSession records applied options and creates a child debugger after the hook succeeds.
func (p *PlanSpy) NewDebugSession(ctx context.Context, options ...api.SessionOption) (debugger.Session, error) {
	configured, err := applyOptions(options)
	if err != nil {
		return nil, err
	}

	p.recorder.record(Call{Resource: p.id, Method: "NewDebugSession", Options: configured})

	if p.behavior.NewDebugSession != nil {
		if err := p.behavior.NewDebugSession(ctx, configured); err != nil {
			return nil, err
		}
	}

	return newDebuggerSpy(p.recorder, p.id, p.behavior.Debugger), nil
}

// Close records entry and settlement around the configured cleanup hook.
func (p *PlanSpy) Close() error {
	p.recorder.record(Call{Resource: p.id, Method: "Close"})
	defer p.recorder.record(Call{Resource: p.id, Method: "CloseFinished"})

	if p.behavior.Close != nil {
		return p.behavior.Close()
	}

	return nil
}
