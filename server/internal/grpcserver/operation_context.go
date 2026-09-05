package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// operationContextFactory resolves the logical owner and combines its lifetime
// with the request. Callers must cancel the returned context after the operation.
type operationContextFactory struct {
	connections *core.ConnectionRegistry
}

func (f *operationContextFactory) New(
	parent context.Context,
	id *wirev1.ConnectionId,
) (*core.Context, context.CancelFunc, error) {
	connection, err := f.connections.Get(core.ConnectionID(id.GetValue()))
	if err != nil {
		return nil, nil, rpcError(err)
	}

	ctx, cancel := core.NewContext(parent, connection)

	return ctx, cancel, nil
}
