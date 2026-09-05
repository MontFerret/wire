package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	"google.golang.org/grpc"
)

// remoteRuntime is a remote implementation of the Universal Ferret API. It owns
// one logical Wire client and borrows the caller's gRPC transport.
type remoteRuntime struct {
	client *Client
}

var _ api.Runtime = (*remoteRuntime)(nil)

// NewRuntime opens a logical Wire connection and exposes it through the
// canonical api.Runtime interface. The private adapter releases its resources
// with bounded detached cleanup; closing it never closes connection.
// On failure, NewRuntime returns a nil Runtime and the connection error.
func NewRuntime(ctx context.Context, connection grpc.ClientConnInterface) (Runtime, error) {
	wireClient, err := New(ctx, connection)
	if err != nil {
		return nil, err
	}

	return &remoteRuntime{client: wireClient}, nil
}

// Run invokes the hosted Runtime.Run operation once and releases the temporary
// Wire execution used to preserve cancellation, output, and failure semantics.
func (r *remoteRuntime) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (Output, error) {
	if r == nil || r.client == nil {
		return Output{}, ErrClosed
	}

	if err := ctx.Err(); err != nil {
		return Output{}, err
	}

	configured, err := applyRuntimeSessionOptions(options)
	if err != nil {
		return Output{}, err
	}

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return Output{}, err
	}

	execution, err := r.client.run(creationCtx, src, configured.parameters, ExecuteOptions{
		OutputContentType: configured.outputContentType,
	})
	cancel()
	if err != nil {
		return Output{}, r.client.reclaimAllocation(ctx, err)
	}

	return execution.waitAndRelease(ctx)
}

// Compile creates a reusable remote Universal API Plan.
func (r *remoteRuntime) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compile(ctx, src, false, options)
}

// CompileDebug creates a reusable remote Plan with debugger metadata.
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
