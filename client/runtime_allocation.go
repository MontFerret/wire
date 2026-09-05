package client

import (
	"context"
	"errors"
)

// reclaimAllocation closes the narrowest known owner of an unknown resource.
// Parent release gates in-flight publication and recursively reclaims children.
// Parents are ordered nearest first; a successful release stops escalation.
// The logical connection is the final owner, and its Connect stream supplies
// the lifetime signal when connection release cannot be acknowledged.
// Known-handle release failures must never enter this allocation-only path.
func (c *Client) reclaimAllocation(ctx context.Context, err error, parents ...func(context.Context) error) error {
	var uncertain *allocationError
	if !errors.As(err, &uncertain) {
		return errors.Join(ctx.Err(), err)
	}

	result := errors.Join(ctx.Err(), err)
	for _, parent := range parents {
		closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, parent)
		if closeErr == nil {
			return result
		}

		result = errors.Join(result, closeErr)
	}

	closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, c.Close)
	if closeErr != nil {
		c.streamCancel()
		c.lifecycleCancel()
	}

	return errors.Join(result, closeErr)
}
