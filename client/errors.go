package client

import (
	"errors"
	"fmt"

	"github.com/MontFerret/api/diagnostics"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/failure"
	"google.golang.org/grpc/status"
)

type (
	// Error is a decoded Wire RPC failure. Category is set only when the server
	// transmitted an ErrorDetail; transport-native statuses leave it zero.
	// Internal transport causes remain available through Unwrap without exposing
	// protocol resource identifiers.
	Error struct {
		Category    failure.Category
		Message     string
		Diagnostics diagnostics.Diagnostics
		cause       error
	}
)

var (
	// ErrClosed reports an operation attempted through a closed Client or resource
	// handle. Closing begins when the first Close call commits teardown.
	ErrClosed = errors.New("Wire client or resource is closed")

	// ErrExecutionCancelled reports that a remote execution reached its cancelled
	// terminal state. It is distinct from cancellation of a Wait caller's context.
	ErrExecutionCancelled = errors.New("remote execution was cancelled")
)

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

	result := &Error{Message: grpcStatus.Message(), cause: err}

	for _, raw := range grpcStatus.Details() {
		switch detail := raw.(type) {
		case *wirev1.ErrorDetail:
			category, conversionErr := convertErrorCategory(detail.GetCategory(), true)
			if conversionErr != nil {
				return conversionErr
			}

			result.Category = category
		case *wirev1.DiagnosticSet:
			converted, conversionErr := convertDiagnosticSet(detail)
			if conversionErr != nil {
				return conversionErr
			}

			result.Diagnostics = converted
		}
	}

	return result
}

func convertFailure(value *wirev1.Failure) (*failure.Failure, error) {
	if value == nil {
		return nil, nil
	}

	convertedDiagnostics, err := convertDiagnosticSet(value.GetDiagnosticSet())
	if err != nil {
		return nil, err
	}

	category, err := convertErrorCategory(value.GetCategory(), false)
	if err != nil {
		return nil, err
	}

	return &failure.Failure{
		Category:    category,
		Message:     value.GetMessage(),
		Diagnostics: convertedDiagnostics,
	}, nil
}

func convertErrorCategory(value wirev1.ErrorCategory, zeroAllowed bool) (failure.Category, error) {
	switch value {
	case wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED:
		if zeroAllowed {
			return 0, nil
		}
	case wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE:
		return failure.CategoryCompilation, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE:
		return failure.CategoryExecution, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_PLAN_NOT_FOUND:
		return failure.CategoryPlanNotFound, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_NOT_FOUND:
		return failure.CategoryExecutionNotFound, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_DEBUG_SESSION_NOT_FOUND:
		return failure.CategoryDebugSessionNotFound, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND:
		return failure.CategoryConnectionNotFound, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE:
		return failure.CategoryInvalidState, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED:
		return failure.CategoryWatcherLagged, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND:
		return failure.CategoryBreakpointNotFound, nil
	case wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE:
		return failure.CategoryInternalRuntime, nil
	}

	return 0, fmt.Errorf("Wire server returned an invalid error category: %d", value)
}
