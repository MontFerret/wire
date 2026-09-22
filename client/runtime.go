package client

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"

	"github.com/MontFerret/api"
)

// remoteRuntime is a remote implementation of the Universal Ferret API. It owns
// one logical Wire client and borrows the caller's gRPC transport.
type remoteRuntime struct {
	client     *connectionHandle
	mu         sync.Mutex
	references int
	closed     bool
	close      closeState
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
func (r *remoteRuntime) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (output *api.Output, resultErr error) {
	if r == nil || r.client == nil {
		return nil, ErrClosed
	}

	if err := r.retain(); err != nil {
		return nil, err
	}

	defer func() { resultErr = errors.Join(resultErr, r.drop()) }()

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

	execution, err := r.client.run(creationCtx, src, configured)
	cancel()

	if err != nil {
		return nil, r.client.reclaimAllocation(ctx, err)
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
) (result api.Plan, resultErr error) {
	if r == nil || r.client == nil {
		return nil, ErrClosed
	}

	if err := r.retain(); err != nil {
		return nil, err
	}

	transferred := false
	defer func() {
		if !transferred {
			resultErr = errors.Join(resultErr, r.drop())
		}
	}()

	if err := runtimeContextError(ctx); err != nil {
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

	plan.owner = r
	transferred = true

	if ctxErr := ctx.Err(); ctxErr != nil {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, plan.Close)

		return nil, errors.Join(ctxErr, closeErr)
	}

	return &remotePlan{plan: plan}, nil
}

// Close gates new root calls. The last retained call or plan releases the logical
// connection; admitted work and descendants keep their independent lifetimes.
func (r *remoteRuntime) Close() error {
	if r == nil || r.client == nil {
		return ErrClosed
	}

	r.mu.Lock()
	started := r.close.Begin()
	r.closed = true
	references := r.references
	r.mu.Unlock()

	if started {
		var err error

		if references == 0 {
			err = boundedCleanup(context.Background(), convenienceCleanupTimeout, r.client.Close)
		}

		r.close.Finish(err)
	}

	return r.close.Wait(context.Background())
}

// retain admits a root call before Close. A compiled plan inherits its reference.
func (r *remoteRuntime) retain() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrClosed
	}

	if err := r.client.checkOpen(); err != nil {
		return err
	}

	r.references++

	return nil
}

// drop drains the logical connection after the final caller-owned resource.
func (r *remoteRuntime) drop() error {
	r.mu.Lock()
	r.references--
	drain := r.closed && r.references == 0
	r.mu.Unlock()

	if drain {
		return boundedCleanup(context.Background(), convenienceCleanupTimeout, r.client.Close)
	}

	return nil
}
