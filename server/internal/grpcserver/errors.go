package grpcserver

import (
	"context"
	"errors"
	"fmt"

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

	category := wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED
	if value := domain.Category(); value != 0 {
		category, conversionErr = failureCategory(value)
		if conversionErr != nil {
			return statusWithCategory(codes.Internal, "internal runtime failure", wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE)
		}
	}

	return statusWithDiagnostics(code, message, category, diagnosticSet)
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
	return diagnosticsToProto(core.DiagnosticsFromError(err))
}

func runtimeConversionError(format string, args ...any) error {
	return &core.DomainError{
		Kind:    core.ErrorKindInternal,
		Message: "internal runtime failure",
		Cause:   fmt.Errorf(format, args...),
	}
}
