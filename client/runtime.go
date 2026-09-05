package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	"google.golang.org/grpc"
)

// Runtime is a remote implementation of the Universal Ferret API. It owns
// one logical Wire client and borrows the caller's gRPC transport.
type Runtime struct {
	client *Client
}

var _ api.Runtime = (*Runtime)(nil)

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

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return api.Output{}, err
	}

	execution, err := r.client.run(creationCtx, src, configured.parameters, ExecuteOptions{
		OutputContentType: configured.outputContentType,
	})
	cancel()
	if err != nil {
		return api.Output{}, r.client.reclaimAllocation(ctx, err, nil)
	}

	return execution.waitAndRelease(ctx)
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

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return nil, err
	}

	plan, err := r.client.Compile(creationCtx, src, CompileOptions{
		Debuggable:           debuggable,
		OptimizationLevel:    configured.optimizationLevel,
		HasOptimizationLevel: configured.hasOptimizationLevel,
	})
	cancel()
	if err != nil {
		return nil, r.client.reclaimAllocation(ctx, err, nil)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := r.client.closeAllocation(ctx, plan.Close, nil)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return &remotePlan{plan: plan}, nil
}

// Close releases the logical Wire connection and all of its remote resources.
func (r *Runtime) Close() error {
	if r == nil || r.client == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, r.client.Close)
}
