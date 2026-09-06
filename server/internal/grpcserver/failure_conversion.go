package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wirefailure "github.com/MontFerret/wire/pkg/failure"
)

func failure(value *wirefailure.Failure) (*wirev1.Failure, error) {
	if value == nil {
		return nil, nil
	}

	diagnosticSet, err := diagnosticsToProto(value.Diagnostics)
	if err != nil {
		return nil, err
	}

	category, err := failureCategory(value.Category)
	if err != nil {
		return nil, err
	}

	return &wirev1.Failure{
		Category:      category,
		Message:       value.Message,
		DiagnosticSet: diagnosticSet,
	}, nil
}

func failureCategory(value wirefailure.Category) (wirev1.ErrorCategory, error) {
	switch value {
	case wirefailure.CategoryCompilation:
		return wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE, nil
	case wirefailure.CategoryExecution:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE, nil
	case wirefailure.CategoryPlanNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_PLAN_NOT_FOUND, nil
	case wirefailure.CategoryExecutionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_NOT_FOUND, nil
	case wirefailure.CategoryDebugSessionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_DEBUG_SESSION_NOT_FOUND, nil
	case wirefailure.CategoryConnectionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND, nil
	case wirefailure.CategoryInvalidState:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE, nil
	case wirefailure.CategoryInternalRuntime:
		return wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE, nil
	case wirefailure.CategoryWatcherLagged:
		return wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED, nil
	case wirefailure.CategoryBreakpointNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND, nil
	case wirefailure.CategorySessionNotFound:
		return wirev1.ErrorCategory_ERROR_CATEGORY_SESSION_NOT_FOUND, nil
	}

	return 0, runtimeConversionError("runtime returned an invalid failure category")
}
