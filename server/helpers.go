package server

import (
	"context"
	"time"
)

func deadlineFrom(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}

	return deadline
}
