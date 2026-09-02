package core

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/google/uuid"
)

// Debugger owns debugger-session creation and ownership-checked lookup.
type Debugger struct {
	plans    *PlanRegistry
	sessions *DebugSessionRegistry
}

func NewDebugger(plans *PlanRegistry, sessions *DebugSessionRegistry) *Debugger {
	return &Debugger{plans: plans, sessions: sessions}
}

func (d *Debugger) Create(ctx *Context, input OpenDebugInput) (DebugSnapshot, error) {
	connection := ctx.Connection()
	if err := connection.beginOperation(); err != nil {
		return DebugSnapshot{}, err
	}

	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return DebugSnapshot{}, err
	}

	owner := connection.ID()
	if err := d.sessions.reserve(owner); err != nil {
		return DebugSnapshot{}, err
	}

	reserved := true
	defer func() {
		if reserved {
			d.sessions.rollback(owner)
		}
	}()

	plan, err := d.plans.beginChild(owner, input.PlanID, true)
	if err != nil {
		return DebugSnapshot{}, err
	}

	defer plan.finishChildCreation()

	options := []api.SessionOption{api.WithParams(cloneParameters(input.Parameters))}
	if input.OutputContentType != "" {
		options = append(options, api.WithOutputContentType(input.OutputContentType))
	}

	runtimeDebugger, err, panicked := openAPIDebugSession(ctx, plan.plan, options)
	if panicked {
		return DebugSnapshot{}, internalError(err)
	}

	if err != nil {
		if !isNil(runtimeDebugger) {
			return DebugSnapshot{}, errors.Join(internalError(err), closeAPIDebugSession(runtimeDebugger))
		}

		return DebugSnapshot{}, internalError(err)
	}

	if isNil(runtimeDebugger) {
		return DebugSnapshot{}, internalError(errors.New("runtime returned no debug session"))
	}

	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, errors.Join(err, closeAPIDebugSession(runtimeDebugger))
	}

	debugCtx, cancel := context.WithCancelCause(connection.Context())
	created := &DebugSession{
		id:             DebugSessionID(uuid.NewString()),
		owner:          owner,
		planID:         plan.id,
		debugger:       runtimeDebugger,
		ctx:            debugCtx,
		cancel:         cancel,
		state:          DebugCreated,
		breakpoints:    make(map[debugger.BreakpointID]debugger.Breakpoint),
		maxWatchers:    d.sessions.maxWatchers,
		maxBreakpoints: d.sessions.maxBreakpoints,
		watchers:       make(map[uint64]*debugWatcher),
	}

	err = d.plans.commitChild(owner, input.PlanID, plan, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		return d.sessions.commit(created)
	})
	if err != nil {
		cancel(context.Canceled)

		return DebugSnapshot{}, errors.Join(err, closeAPIDebugSession(runtimeDebugger))
	}

	reserved = false

	return created.snapshot(), nil
}

func openAPIDebugSession(
	ctx context.Context,
	plan api.Plan,
	options []api.SessionOption,
) (session debugger.Session, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			session = nil
			err = errors.New("runtime debug session creation panicked")
			panicked = true
		}
	}()

	session, err = plan.NewDebugSession(ctx, options...)

	return
}

func (d *Debugger) Session(ctx *Context, id DebugSessionID) (*DebugSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return d.sessions.get(ctx.connectionID(), id)
}
