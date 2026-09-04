package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/server/internal/panicboundary"
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

func (d *Debugger) Create(ctx *Context, input OpenDebugInput) (DebugSessionRecord, error) {
	connection := ctx.Connection()
	if err := connection.beginOperation(); err != nil {
		return DebugSessionRecord{}, err
	}

	defer connection.finishOperation()

	if err := ctx.Err(); err != nil {
		return DebugSessionRecord{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return DebugSessionRecord{}, err
	}

	owner := connection.ID()
	if err := d.sessions.reserve(owner); err != nil {
		return DebugSessionRecord{}, err
	}

	reserved := true
	defer func() {
		if reserved {
			d.sessions.rollback(owner)
		}
	}()

	plan, err := d.plans.beginChild(owner, input.PlanID, true)
	if err != nil {
		return DebugSessionRecord{}, err
	}

	defer plan.finishChildCreation()

	options := []api.SessionOption{api.WithParams(cloneParameters(input.Parameters))}
	if input.OutputContentType != "" {
		options = append(options, api.WithOutputContentType(input.OutputContentType))
	}

	runtimeDebugger, err := panicboundary.Call(func() (debugger.Session, error) {
		return plan.plan.NewDebugSession(ctx, options...)
	})
	if err != nil {
		var panicErr *panicboundary.Error
		if errors.As(err, &panicErr) {
			return DebugSessionRecord{}, internalError(fmt.Errorf("create runtime debug session: %w", err))
		}
	}

	var controller *DebugController
	if !isNil(runtimeDebugger) {
		controller = newDebugController(runtimeDebugger)
	}

	if err != nil {
		if controller != nil {
			return DebugSessionRecord{}, errors.Join(internalError(err), controller.Close())
		}

		return DebugSessionRecord{}, internalError(err)
	}

	if controller == nil {
		return DebugSessionRecord{}, internalError(errors.New("runtime returned no debug session"))
	}

	if err := ctx.Err(); err != nil {
		return DebugSessionRecord{}, errors.Join(err, controller.Close())
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

		return DebugSessionRecord{}, errors.Join(err, controller.Close())
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
