package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MontFerret/wire/internal/lifecycle"
)

const convenienceCleanupTimeout = 30 * time.Second

func boundedCleanup(ctx context.Context, timeout time.Duration, close func(context.Context) error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	err := close(cleanupCtx)
	if ctxErr := cleanupCtx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		err = errors.Join(err, ctxErr)
	}

	return err
}

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
