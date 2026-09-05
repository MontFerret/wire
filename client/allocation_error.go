package client

import (
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
