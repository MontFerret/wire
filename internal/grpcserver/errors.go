package grpcserver

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func rpcError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, context.Canceled.Error())
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())
	}

	if errors.Is(err, core.ErrWatcherLagged) {
		return statusWithCategory(
			codes.ResourceExhausted,
			"watcher lagged behind the event stream",
			wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED,
		)
	}

	var domain *core.DomainError
	if !errors.As(err, &domain) {
		return statusWithCategory(
			codes.Internal,
			"internal runtime failure",
			wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE,
		)
	}

	code := codes.Internal
	switch domain.Category {
	case core.ErrorInvalidRequest, core.ErrorCompilation:
		code = codes.InvalidArgument
	case core.ErrorPlanNotFound, core.ErrorExecutionNotFound, core.ErrorDebugSessionNotFound,
		core.ErrorConnectionNotFound, core.ErrorValueReferenceNotFound, core.ErrorBreakpointNotFound:
		code = codes.NotFound
	case core.ErrorInvalidState:
		code = codes.FailedPrecondition
	case core.ErrorUnsupported:
		code = codes.Unimplemented
	case core.ErrorWatcherLagged, core.ErrorResourceExhausted:
		code = codes.ResourceExhausted
	}

	message := domain.Message
	if message == "" || domain.Category == core.ErrorInternal {
		message = "internal runtime failure"
	}

	return statusWithCategory(code, message, errorCategory(domain.Category))
}

func statusWithCategory(code codes.Code, message string, category wirev1.ErrorCategory) error {
	base := status.New(code, message)
	if category == wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED {
		return base.Err()
	}

	withDetails, err := base.WithDetails(&wirev1.ErrorDetail{Category: category})
	if err != nil {
		return base.Err()
	}

	return withDetails.Err()
}

func errorCategory(value core.ErrorCategory) wirev1.ErrorCategory {
	switch value {
	case core.ErrorCompilation:
		return wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE
	case core.ErrorExecution:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE
	case core.ErrorPlanNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_PLAN_NOT_FOUND
	case core.ErrorExecutionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_NOT_FOUND
	case core.ErrorDebugSessionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_DEBUG_SESSION_NOT_FOUND
	case core.ErrorConnectionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND
	case core.ErrorInvalidState:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE
	case core.ErrorWatcherLagged:
		return wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED
	case core.ErrorValueReferenceNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_VALUE_REFERENCE_NOT_FOUND
	case core.ErrorBreakpointNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND
	case core.ErrorInternal:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE
	default:
		return wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED
	}
}
