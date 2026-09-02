package core

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
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

	var controller *DebugController
	if !isNil(runtimeDebugger) {
		controller = newDebugController(runtimeDebugger)
	}

	if err != nil {
		if controller != nil {
			return DebugSnapshot{}, errors.Join(internalError(err), controller.Close())
		}

		return DebugSnapshot{}, internalError(err)
	}

	if controller == nil {
		return DebugSnapshot{}, internalError(errors.New("runtime returned no debug session"))
	}

	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, errors.Join(err, controller.Close())
	}

	debugCtx, cancel := context.WithCancelCause(connection.Context())
	created := newDebugSession(
		DebugSessionID(uuid.NewString()),
		owner,
		plan.id,
		controller,
		debugCtx,
		cancel,
		d.sessions.maxWatchers,
		d.sessions.maxBreakpoints,
	)

	err = d.plans.commitChild(owner, input.PlanID, plan, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		return d.sessions.commit(created)
	})
	if err != nil {
		cancel(context.Canceled)

		return DebugSnapshot{}, errors.Join(err, controller.Close())
	}

	reserved = false

	return created.snapshot(), nil
}

func (d *Debugger) Session(ctx *Context, id DebugSessionID) (*DebugSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return d.sessions.get(ctx.connectionID(), id)
}
