package client

import (
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

	code := grpcStatus.Code()
	result := &Error{Message: grpcStatus.Message(), cause: err}
	for _, raw := range grpcStatus.Details() {
		detail, ok := raw.(*wirev1.ErrorDetail)
		if !ok {
			continue
		}

		result.Category = clientErrorCategory(detail.GetCategory())
		result.Message = detail.GetMessage()
		result.Diagnostics = convertDiagnostics(detail.GetDiagnostics())

		break
	}

	if result.Category == 0 && (code == codes.Canceled || code == codes.DeadlineExceeded) {
		result.Category = ErrorCancelled
	}

	return result
}

func convertFailure(value *wirev1.Failure) *Failure {
	if value == nil {
		return nil
	}

	return &Failure{
		Category:    clientErrorCategory(value.GetCategory()),
		Message:     value.GetMessage(),
		Diagnostics: convertDiagnostics(value.GetDiagnostics()),
	}
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

func convertDiagnostics(values []*wirev1.Diagnostic) []Diagnostic {
	result := make([]Diagnostic, len(values))

	for i, value := range values {
		if value == nil {
			continue
		}

		spans := make([]DiagnosticSpan, len(value.GetSpans()))
		for j, span := range value.GetSpans() {
			if span == nil {
				continue
			}

			spans[j] = DiagnosticSpan{
				Start:   span.GetStartByte(),
				End:     span.GetEndByte(),
				Label:   span.GetLabel(),
				Primary: span.GetPrimary(),
			}
		}

		result[i] = Diagnostic{
			Kind:           value.GetKind(),
			Message:        value.GetMessage(),
			Hint:           value.GetHint(),
			Note:           value.GetNote(),
			SourceIdentity: value.GetSourceIdentity(),
			Spans:          spans,
		}
	}

	return result
}
