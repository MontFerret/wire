package client

import "context"

func retainedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.Background(), func() {}
	}

	return context.WithDeadline(context.Background(), deadline)
}
