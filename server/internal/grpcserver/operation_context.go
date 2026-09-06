package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func prepareOperation(parent context.Context, connections *core.ConnectionRegistry, id *wirev1.ConnectionId) (context.Context, *core.ResourceStore, context.CancelFunc, error) {
	connection, err := connections.Get(core.ConnectionID(id.GetValue()))
	if err != nil {
		return nil, nil, nil, rpcError(err)
	}

	ctx, cancel := core.OperationContext(parent, connection.Context())

	return ctx, connection.Resources(), cancel, nil
}
