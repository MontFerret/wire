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
		return statusWithDetail(codes.Canceled, &wirev1.ErrorDetail{
			Category: wirev1.ErrorCategory_ERROR_CATEGORY_CANCELLED,
			Message:  context.Canceled.Error(),
		})
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return statusWithDetail(codes.DeadlineExceeded, &wirev1.ErrorDetail{
			Category: wirev1.ErrorCategory_ERROR_CATEGORY_CANCELLED,
			Message:  context.DeadlineExceeded.Error(),
		})
	}
	if errors.Is(err, core.ErrWatcherLagged) {
		return statusWithDetail(codes.ResourceExhausted, &wirev1.ErrorDetail{
			Category: wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED,
			Message:  "watcher lagged behind the event stream",
			Resource: wirev1.ResourceKind_RESOURCE_KIND_WATCHER,
		})
	}

	var domain *core.DomainError
	if !errors.As(err, &domain) {
		return statusWithDetail(codes.Internal, &wirev1.ErrorDetail{
			Category: wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE,
			Message:  "internal runtime failure",
		})
	}

	code := codes.Internal
	switch domain.Category {
	case core.ErrorInvalidRequest, core.ErrorCompilation:
		code = codes.InvalidArgument
	case core.ErrorPlanNotFound, core.ErrorExecutionNotFound, core.ErrorDebugSessionNotFound, core.ErrorConnectionNotFound, core.ErrorValueReferenceNotFound:
		code = codes.NotFound
	case core.ErrorInvalidState:
		code = codes.FailedPrecondition
	case core.ErrorUnsupported:
		code = codes.Unimplemented
	case core.ErrorWatcherLagged:
		code = codes.ResourceExhausted
	}

	detail := &wirev1.ErrorDetail{
		Category:    errorCategory(domain.Category),
		Message:     domain.Message,
		Resource:    resourceKind(domain.Category),
		ResourceId:  domain.ResourceID,
		Diagnostics: diagnostics(domain.Diagnostics),
	}
	if detail.Message == "" || domain.Category == core.ErrorInternal {
		detail.Message = "internal runtime failure"
	}

	return statusWithDetail(code, detail)
}

func statusWithDetail(code codes.Code, detail *wirev1.ErrorDetail) error {
	withDetails, err := status.New(code, detail.GetMessage()).WithDetails(detail)
	if err != nil {
		return status.Error(code, detail.GetMessage())
	}
	return withDetails.Err()
}

func errorCategory(value core.ErrorCategory) wirev1.ErrorCategory {
	switch value {
	case core.ErrorInvalidRequest:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_REQUEST
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
	case core.ErrorUnsupported:
		return wirev1.ErrorCategory_ERROR_CATEGORY_UNSUPPORTED_CAPABILITY
	case core.ErrorWatcherLagged:
		return wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED
	case core.ErrorValueReferenceNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_VALUE_REFERENCE_NOT_FOUND
	default:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE
	}
}

func resourceKind(value core.ErrorCategory) wirev1.ResourceKind {
	switch value {
	case core.ErrorConnectionNotFound:
		return wirev1.ResourceKind_RESOURCE_KIND_CONNECTION
	case core.ErrorPlanNotFound:
		return wirev1.ResourceKind_RESOURCE_KIND_PLAN
	case core.ErrorExecutionNotFound:
		return wirev1.ResourceKind_RESOURCE_KIND_EXECUTION
	case core.ErrorDebugSessionNotFound:
		return wirev1.ResourceKind_RESOURCE_KIND_DEBUG_SESSION
	case core.ErrorWatcherLagged:
		return wirev1.ResourceKind_RESOURCE_KIND_WATCHER
	case core.ErrorValueReferenceNotFound:
		return wirev1.ResourceKind_RESOURCE_KIND_VALUE_REFERENCE
	default:
		return wirev1.ResourceKind_RESOURCE_KIND_UNSPECIFIED
	}
}
