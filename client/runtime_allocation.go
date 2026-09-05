package client

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// allocationError marks an RPC whose resource may have committed without a
// usable handle reaching the caller. Local validation never produces it.
type allocationError struct {
	cause error
}

func (e *allocationError) Error() string {
	return e.cause.Error()
}

func (e *allocationError) Unwrap() error {
	return e.cause
}

func allocationRPCError(err error) error {
	decoded := decodeError(err)
	var rejection *Error
	if errors.As(decoded, &rejection) && rejection.Category != 0 {
		// Structured Wire failures describe a rejected creation. Its owner rolls
		// back before returning; a transport-native failure gives no such proof.
		return decoded
	}

	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition,
		codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented:
		return decoded
	default:
		// ResourceExhausted can also mean the committed response exceeded the
		// transport's receive limit, so it cannot prove allocation was rejected.
		return &allocationError{cause: decoded}
	}
}

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

// reclaimAllocation closes the narrowest known owner of an unknown resource.
// Parent release gates in-flight publication and recursively reclaims children.
// An uncertain parent release escalates to this logical connection, whose
// Connect stream is the final lifetime signal independent of release RPCs.
func (c *Client) reclaimAllocation(ctx context.Context, err error, parent func(context.Context) error) error {
	var uncertain *allocationError
	if !errors.As(err, &uncertain) {
		return errors.Join(ctx.Err(), err)
	}

	result := errors.Join(ctx.Err(), err)
	if parent != nil {
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

// closeAllocation reclaims an acquired handle, escalating if its release cannot
// establish cleanup. All release attempts use the existing retained close state.
func (c *Client) closeAllocation(ctx context.Context, release, parent func(context.Context) error) error {
	err := boundedCleanup(ctx, convenienceCleanupTimeout, release)
	if err == nil {
		return nil
	}

	return c.reclaimAllocation(ctx, &allocationError{cause: err}, parent)
}
