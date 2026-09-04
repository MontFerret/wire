package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"google.golang.org/grpc"
)

type (
	// Runtime is a remote implementation of the Universal Ferret API. It owns
	// one logical Wire client and borrows the caller's gRPC transport.
	Runtime struct {
		client *Client
	}

	runtimePlan struct {
		plan *Plan
	}

	runtimeSession struct {
		session *sessionHandle
	}
)

var (
	_ api.Runtime      = (*Runtime)(nil)
	_ api.Plan         = (*runtimePlan)(nil)
	_ api.Session      = (*runtimeSession)(nil)
	_ debugger.Session = (*runtimeDebugSession)(nil)
)

// NewRuntime opens a logical Wire connection and exposes it through the
// Universal Ferret API. Closing the Runtime never closes connection.
func NewRuntime(ctx context.Context, connection grpc.ClientConnInterface) (*Runtime, error) {
	wireClient, err := New(ctx, connection)
	if err != nil {
		return nil, err
	}

	return &Runtime{client: wireClient}, nil
}

// Run invokes the hosted Runtime.Run operation once and releases the temporary
// Wire execution used to preserve cancellation, output, and failure semantics.
func (r *Runtime) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (api.Output, error) {
	if r == nil || r.client == nil {
		return api.Output{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return api.Output{}, err
	}

	configured, err := applyRuntimeSessionOptions(options)
	if err != nil {
		return api.Output{}, err
	}

	creationCtx, cancel := runtimeAllocationContext(ctx)
	execution, err := r.client.runRuntime(creationCtx, src, configured.parameters, ExecuteOptions{
		OutputContentType: configured.outputContentType,
	})
	cancel()
	if err != nil {
		return api.Output{}, err
	}

	return waitAndReleaseExecution(ctx, execution)
}

// Compile creates a reusable remote Universal API Plan.
func (r *Runtime) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compile(ctx, src, false, options)
}

// CompileDebug creates a reusable remote Plan with debugger metadata.
func (r *Runtime) CompileDebug(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compile(ctx, src, true, options)
}

func (r *Runtime) compile(
	ctx context.Context,
	src api.Source,
	debuggable bool,
	options []api.PlanOption,
) (api.Plan, error) {
	if r == nil || r.client == nil {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	configured, err := applyRuntimePlanOptions(options)
	if err != nil {
		return nil, err
	}

	creationCtx, cancel := runtimeAllocationContext(ctx)
	plan, err := r.client.Compile(creationCtx, src, CompileOptions{
		Debuggable:        debuggable,
		OptimizationLevel: configured.optimizationLevel,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, plan.Close)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return &runtimePlan{plan: plan}, nil
}

// Close releases the logical Wire connection and all of its remote resources.
func (r *Runtime) Close() error {
	if r == nil || r.client == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, r.client.Close)
}

func (p *runtimePlan) Params() []string {
	if p == nil || p.plan == nil {
		return nil
	}

	return p.plan.Parameters()
}

func (p *runtimePlan) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	if p == nil || p.plan == nil {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	configured, err := applyRuntimeSessionOptions(options)
	if err != nil {
		return nil, err
	}

	creationCtx, cancel := runtimeAllocationContext(ctx)
	session, err := p.plan.newRuntimeSession(creationCtx, configured.parameters, ExecuteOptions{
		OutputContentType: configured.outputContentType,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, session.Close)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return &runtimeSession{session: session}, nil
}

func (p *runtimePlan) NewDebugSession(
	ctx context.Context,
	options ...api.SessionOption,
) (debugger.Session, error) {
	if p == nil || p.plan == nil {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	configured, err := applyRuntimeSessionOptions(options)
	if err != nil {
		return nil, err
	}

	creationCtx, cancel := runtimeAllocationContext(ctx)
	session, err := p.plan.NewDebugSession(creationCtx, configured.parameters, DebugSessionOptions{
		OutputContentType: configured.outputContentType,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, session.Close)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return newRuntimeDebugSession(session), nil
}

func (p *runtimePlan) Close() error {
	if p == nil || p.plan == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, p.plan.Close)
}

func (s *runtimeSession) Run(ctx context.Context) (api.Output, error) {
	if s == nil || s.session == nil {
		return api.Output{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return api.Output{}, err
	}

	creationCtx, cancel := runtimeAllocationContext(ctx)
	execution, err := s.session.run(creationCtx)
	cancel()
	if err != nil {
		return api.Output{}, err
	}

	return waitAndReleaseExecution(ctx, execution)
}

func (s *runtimeSession) Close() error {
	if s == nil || s.session == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, s.session.Close)
}

func waitAndReleaseExecution(ctx context.Context, execution *Execution) (api.Output, error) {
	output, waitErr := execution.Wait(ctx)
	var cancelErr error
	if ctx.Err() != nil {
		cancelErr = boundedCleanup(ctx, convenienceCleanupTimeout, execution.Cancel)
	}
	closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, execution.Close)

	return output, errors.Join(waitErr, cancelErr, closeErr)
}

func runtimeAllocationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// Allocation responses carry the only handle capable of releasing a resource.
	// Keep that short RPC alive through caller cancellation, then observe the
	// caller context and release any resource whose response raced cancellation.
	return context.WithTimeout(context.WithoutCancel(ctx), convenienceCleanupTimeout)
}
