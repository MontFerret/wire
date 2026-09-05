package client

import (
	"context"
)

func runtimeAllocationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	// The response carries the only handle for targeted release. Preserve this
	// acquisition RPC through caller cancellation, but never past this bound.
	// The adapter observes the original context again when the handle arrives.
	allocation, cancel := context.WithTimeout(context.WithoutCancel(ctx), convenienceCleanupTimeout)

	return allocation, cancel, nil
}
