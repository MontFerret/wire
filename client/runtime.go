package client

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/MontFerret/api"
)

// remoteRuntime is a remote implementation of the Universal Ferret API. It owns
// one logical Wire client and borrows the caller's gRPC transport.
type remoteRuntime struct {
	client *connectionHandle
}

var _ api.Runtime = (*remoteRuntime)(nil)

// New opens a logical Wire connection and exposes it through the
// canonical api.Runtime interface. The context bounds the handshake; cancelling
// it after construction does not close the runtime. Close releases the logical
// connection and its resources with bounded detached cleanup, leaving the
// caller-owned transport open. On failure, New returns a nil interface.
func New(ctx context.Context, connection grpc.ClientConnInterface) (api.Runtime, error) {
	wireClient, err := newConnection(ctx, connection)
	if err != nil {
		return nil, err
	}

	return &remoteRuntime{client: wireClient}, nil
}

// Run invokes the hosted api.Runtime.Run operation once and releases the temporary
// Wire execution used to preserve cancellation, output, and failure semantics.
func (r *remoteRuntime) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (api.Output, error) {
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

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return api.Output{}, err
	}

	execution, err := r.client.run(creationCtx, src, configured)
	cancel()

	if err != nil {
		return api.Output{}, r.client.reclaimAllocation(ctx, err)
	}

	return execution.waitAndRelease(ctx)
}

// Compile creates a reusable remote Universal API plan.
func (r *remoteRuntime) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compile(ctx, src, false, options)
}

// CompileDebug creates a reusable remote plan with debugger metadata.
func (r *remoteRuntime) CompileDebug(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compile(ctx, src, true, options)
}

func (r *remoteRuntime) compile(
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

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return nil, err
	}

	plan, err := r.client.compileConfigured(creationCtx, src, debuggable, configured)
	cancel()

	if err != nil {
		return nil, r.client.reclaimAllocation(ctx, err)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, plan.Close)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return &remotePlan{plan: plan}, nil
}

// Close releases the logical Wire connection and all of its remote resources.
func (r *remoteRuntime) Close() error {
	if r == nil || r.client == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, r.client.Close)
}
