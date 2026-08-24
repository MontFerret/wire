package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/MontFerret/wire/internal/lifecycle"
)

func retainedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.Background(), func() {}
	}

	return context.WithDeadline(context.Background(), deadline)
}

func settleHandleClose(ctx context.Context, kind string, state *lifecycle.Close, release func(context.Context) error) {
	retained, cancel := retainedContext(ctx)
	defer cancel()

	var result error
	defer func() {
		if recover() != nil {
			result = errors.Join(result, fmt.Errorf("Wire %s close panicked", kind))
		}

		state.Finish(result)
	}()

	result = release(retained)
}
