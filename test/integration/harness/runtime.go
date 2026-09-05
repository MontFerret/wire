package harness

import (
	"context"

	"github.com/MontFerret/api"
)

type (
	// RuntimeBehavior is configured before the server starts. Hooks run outside locks.
	RuntimeBehavior struct {
		Run     func(context.Context, api.Source, SessionOptions) (api.Output, error)
		Compile func(context.Context, api.Source, bool, CompileOptions) error
		Plan    PlanBehavior
	}

	RuntimeSpy struct {
		id       int
		recorder *Recorder
		behavior RuntimeBehavior
	}
)

var _ api.Runtime = (*RuntimeSpy)(nil)

func NewRuntimeSpy(behavior RuntimeBehavior) *RuntimeSpy {
	recorder := newRecorder()

	return &RuntimeSpy{id: recorder.create("runtime", 0), recorder: recorder, behavior: behavior}
}

func (r *RuntimeSpy) Recorder() *Recorder {
	return r.recorder
}

func (r *RuntimeSpy) ID() int {
	return r.id
}

func (r *RuntimeSpy) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (api.Output, error) {
	configured, err := applyOptions(options)
	if err != nil {
		return api.Output{}, err
	}

	r.recorder.record(Call{Resource: r.id, Method: "Run", Source: src, Options: configured})
	defer r.recorder.record(Call{Resource: r.id, Method: "RunFinished"})

	if r.behavior.Run != nil {
		return r.behavior.Run(ctx, src, configured)
	}

	return api.Output{}, nil
}

func (r *RuntimeSpy) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compile(ctx, src, false, options)
}

func (r *RuntimeSpy) CompileDebug(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compile(ctx, src, true, options)
}

func (r *RuntimeSpy) compile(ctx context.Context, src api.Source, debug bool, options []api.PlanOption) (api.Plan, error) {
	var configured CompileOptions

	for _, option := range options {
		if option != nil {
			if err := option(&configured); err != nil {
				return nil, err
			}
		}
	}

	method := "Compile"

	if debug {
		method = "CompileDebug"
	}

	r.recorder.record(Call{Resource: r.id, Method: method, Source: src, Compile: configured})
	defer r.recorder.record(Call{Resource: r.id, Method: method + "Finished"})

	if r.behavior.Compile != nil {
		if err := r.behavior.Compile(ctx, src, debug, configured); err != nil {
			return nil, err
		}
	}

	return &PlanSpy{id: r.recorder.create("plan", r.id), recorder: r.recorder, behavior: r.behavior.Plan}, nil
}

func (r *RuntimeSpy) Close() error {
	r.recorder.record(Call{Resource: r.id, Method: "Close"})

	return nil
}
