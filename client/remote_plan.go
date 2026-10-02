package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

type remotePlan struct {
	plan *planHandle
}

var _ api.Plan = (*remotePlan)(nil)

// Params returns a caller-owned copy of the compile snapshot, including after
// plan or transport cleanup. Context validation requires no lifecycle admission.
func (p *remotePlan) Params(ctx context.Context) ([]string, error) {
	if p == nil || p.plan == nil {
		return nil, ErrClosed
	}

	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	return p.plan.Parameters(), nil
}

func (p *remotePlan) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	if p == nil || p.plan == nil {
		return nil, ErrClosed
	}

	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	configured, err := applyRuntimeSessionOptions(options)
	if err != nil {
		return nil, err
	}

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return nil, err
	}

	session, err := p.plan.newSession(creationCtx, configured)
	cancel()

	if err != nil {
		return nil, p.plan.client.reclaimAllocation(ctx, err, p.plan.Close)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, session.Close)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return &remoteSession{session: session}, nil
}

func (p *remotePlan) NewDebugSession(
	ctx context.Context,
	options ...api.SessionOption,
) (debugger.Session, error) {
	if p == nil || p.plan == nil {
		return nil, ErrClosed
	}

	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	configured, err := applyRuntimeSessionOptions(options)
	if err != nil {
		return nil, err
	}

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return nil, err
	}

	session, err := p.plan.NewDebugSession(creationCtx, configured)
	cancel()

	if err != nil {
		return nil, p.plan.client.reclaimAllocation(ctx, err, p.plan.Close)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, session.Close)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return newRemoteDebugSession(session), nil
}

func (p *remotePlan) Close() error {
	if p == nil || p.plan == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, p.plan.closeAPI)
}
