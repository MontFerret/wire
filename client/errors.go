package client

import (
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/status"
)

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

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
	default:
		return ErrorInternal
	}
}
