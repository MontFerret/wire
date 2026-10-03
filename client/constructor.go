package client

import (
	"context"
	"errors"
	"strings"

	gooptions "github.com/ziflex/go-options"
	"google.golang.org/grpc"

	"github.com/MontFerret/api"
)

// New creates and owns a gRPC channel and completes the Wire handshake before
// returning a remote runtime. Transport defaults to TLS with system trust and
// server identity verification; plaintext requires WithInsecure.
// The context controls startup only. Cancellation after publication leaves the
// runtime usable. Failed startup returns a nil interface and performs bounded,
// detached rollback, which may take additional time after cancellation.
// Close gates new root calls; the channel closes after final logical release,
// including release deferred to admitted work or caller-owned descendants.
func New(ctx context.Context, target string, options ...Option) (api.Runtime, error) {
	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	if strings.TrimSpace(target) == "" {
		return nil, errors.New("gRPC target must not be empty")
	}

	configured, err := configure(options)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	connection, err := grpc.NewClient(target, configured.dialOptions()...)
	if err != nil {
		return nil, err
	}

	return newRuntime(ctx, connection, connection.Close)
}

// From completes the Wire handshake over a caller-owned gRPC connection.
// It accepts any non-nil ClientConnInterface and never closes the supplied
// transport, including on startup failure or logical-resource recovery.
// The context controls startup, not the returned runtime's lifetime. Failure
// returns a nil interface; bounded detached rollback may outlast cancellation.
func From(ctx context.Context, connection grpc.ClientConnInterface) (api.Runtime, error) {
	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	if err := gooptions.NotNil[grpc.ClientConnInterface]()(connection); err != nil {
		return nil, errors.New("gRPC connection is required")
	}

	return newRuntime(ctx, connection, nil)
}

// newRuntime transfers transport teardown to the existing logical connection
// owner before startup. Rollback and published-runtime release share that owner.
func newRuntime(ctx context.Context, connection grpc.ClientConnInterface, transportClose func() error) (api.Runtime, error) {
	wireClient, err := newConnection(ctx, connection, transportClose)
	if err != nil {
		return nil, err
	}

	return &remoteRuntime{client: wireClient}, nil
}
