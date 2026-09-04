package grpcserver

import (
	"context"
	"errors"

	"github.com/MontFerret/api/diagnostics"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
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
	switch domain.Kind {
	case core.ErrorKindInvalidRequest, core.ErrorKindCompilation:
		code = codes.InvalidArgument
	case core.ErrorKindPlanNotFound, core.ErrorKindExecutionNotFound, core.ErrorKindDebugSessionNotFound,
		core.ErrorKindConnectionNotFound, core.ErrorKindBreakpointNotFound, core.ErrorKindSessionNotFound:
		code = codes.NotFound
	case core.ErrorKindInvalidState:
		code = codes.FailedPrecondition
	case core.ErrorKindUnsupported:
		code = codes.Unimplemented
	case core.ErrorKindWatcherLagged, core.ErrorKindResourceExhausted:
		code = codes.ResourceExhausted
	}

	message := domain.Message
	if message == "" || domain.Kind == core.ErrorKindInternal {
		message = "internal runtime failure"
	}

	diagnosticSet, conversionErr := diagnosticSetFromError(err)
	if conversionErr != nil {
		return statusWithCategory(
			codes.Internal,
			"internal runtime failure",
			wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE,
		)
	}

	return statusWithDiagnostics(code, message, errorCategory(domain.Kind), diagnosticSet)
}

func statusWithCategory(code codes.Code, message string, category wirev1.ErrorCategory) error {
	return statusWithDiagnostics(code, message, category, nil)
}

func statusWithDiagnostics(
	code codes.Code,
	message string,
	category wirev1.ErrorCategory,
	diagnosticSet *wirev1.DiagnosticSet,
) error {
	base := status.New(code, message)
	if category == wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED {
		return base.Err()
	}

	withDetails, err := base.WithDetails(&wirev1.ErrorDetail{Category: category})
	if err != nil {
		return base.Err()
	}

	if diagnosticSet != nil {
		withDiagnostics, err := withDetails.WithDetails(diagnosticSet)
		if err != nil {
			return withDetails.Err()
		}

		withDetails = withDiagnostics
	}

	return withDetails.Err()
}

func diagnosticSetFromError(err error) (*wirev1.DiagnosticSet, error) {
	var values diagnostics.Diagnostics
	if errors.As(err, &values) {
		return diagnosticsToProto(values)
	}

	var pointer *diagnostics.Diagnostics
	if errors.As(err, &pointer) && pointer != nil {
		return diagnosticsToProto(*pointer)
	}

	return nil, nil
}

func errorCategory(value core.ErrorKind) wirev1.ErrorCategory {
	switch value {
	case core.ErrorKindCompilation:
		return wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE
	case core.ErrorKindExecution:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE
	case core.ErrorKindPlanNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_PLAN_NOT_FOUND
	case core.ErrorKindExecutionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_NOT_FOUND
	case core.ErrorKindDebugSessionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_DEBUG_SESSION_NOT_FOUND
	case core.ErrorKindConnectionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND
	case core.ErrorKindInvalidState:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE
	case core.ErrorKindWatcherLagged:
		return wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED
	case core.ErrorKindBreakpointNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND
	case core.ErrorKindInternal:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE
	case core.ErrorKindSessionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_SESSION_NOT_FOUND
	default:
		return wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED
	}
}
