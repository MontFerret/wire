package server

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
)

func deadlineFrom(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}

	return deadline
}

func normalizeServeError(err error) error {
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}

	return err
}
