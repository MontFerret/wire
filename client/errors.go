package client

import (
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrClosed reports an operation attempted through a closed Client or resource
	// handle. Closing begins when the first Close call commits teardown.
	ErrClosed = errors.New("Wire client or resource is closed")

	// ErrExecutionCancelled reports that a remote execution reached its cancelled
	// terminal state. It is distinct from cancellation of a Wait caller's context.
	ErrExecutionCancelled = errors.New("remote execution was cancelled")
)

// Error returns the sanitized terminal failure message.
func (f *Failure) Error() string {
	if f == nil {
		return ""
	}

	return f.Message
}

// Error returns the sanitized Wire error message.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}

	return e.Message
}

// Unwrap returns the underlying transport error for errors.Is and errors.As.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.cause
}

func decodeError(err error) error {
	if err == nil {
		return nil
	}
	var existing *Error
	if errors.As(err, &existing) {
		return err
	}

	grpcStatus, ok := status.FromError(err)
	if !ok {
		return err
	}
	result := &Error{Code: grpcStatus.Code(), Message: grpcStatus.Message(), cause: err}
	for _, raw := range grpcStatus.Details() {
		detail, ok := raw.(*wirev1.ErrorDetail)
		if !ok {
			continue
		}
		result.Category = clientErrorCategory(detail.GetCategory())
		result.Message = detail.GetMessage()
		result.ResourceID = detail.GetResourceId()
		result.Diagnostics = convertDiagnostics(detail.GetDiagnostics())
		break
	}

	if result.Category == 0 && (result.Code == codes.Canceled || result.Code == codes.DeadlineExceeded) {
		result.Category = ErrorCancelled
	}

	return result
}

func clientErrorCategory(value wirev1.ErrorCategory) ErrorCategory {
	switch value {
	case wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_REQUEST:
		return ErrorInvalidRequest
	case wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE:
		return ErrorCompilation
	case wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE:
		return ErrorExecution
	case wirev1.ErrorCategory_ERROR_CATEGORY_PLAN_NOT_FOUND:
		return ErrorPlanNotFound
	case wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_NOT_FOUND:
		return ErrorExecutionNotFound
	case wirev1.ErrorCategory_ERROR_CATEGORY_DEBUG_SESSION_NOT_FOUND:
		return ErrorDebugSessionNotFound
	case wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND:
		return ErrorConnectionNotFound
	case wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE:
		return ErrorInvalidState
	case wirev1.ErrorCategory_ERROR_CATEGORY_UNSUPPORTED_CAPABILITY:
		return ErrorUnsupported
	case wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED:
		return ErrorWatcherLagged
	case wirev1.ErrorCategory_ERROR_CATEGORY_CANCELLED:
		return ErrorCancelled
	case wirev1.ErrorCategory_ERROR_CATEGORY_VALUE_REFERENCE_NOT_FOUND:
		return ErrorValueReferenceNotFound
	case wirev1.ErrorCategory_ERROR_CATEGORY_RESOURCE_EXHAUSTED:
		return ErrorResourceExhausted
	case wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND:
		return ErrorBreakpointNotFound
	default:
		return ErrorInternal
	}
}
