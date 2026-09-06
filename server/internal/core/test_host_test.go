package core

import (
	"context"

	"github.com/MontFerret/api"
)

type testHost struct {
	connections *ConnectionRegistry
	runtime     api.Runtime
}

func newTestHost(runtime api.Runtime, limits fixtureLimits) (*testHost, error) {
	if isNil(runtime) {
		return nil, invalidRequest("runtime is required")
	}

	return &testHost{
		runtime:     runtime,
		connections: NewConnectionRegistry(limits.MaxConnections, limits.resources()),
	}, nil
}

func (h *testHost) OpenConnection() (*testEnvironment, error) {
	connection, err := h.connections.Open()
	if err != nil {
		return nil, err
	}

	return &testEnvironment{Connection: connection, host: h}, nil
}

func (h *testHost) CloseConnection(ctx context.Context, id ConnectionID) error {
	return h.connections.CloseConnection(ctx, id)
}

func (h *testHost) Close(ctx context.Context) error {
	return h.connections.Close(ctx)
}
