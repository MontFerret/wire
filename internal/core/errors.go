package core

import (
	"errors"

	"github.com/MontFerret/api/diagnostics"
)

var ErrWatcherLagged = errors.New("wire watcher lagged")

func invalidRequest(message string) error {
	return &DomainError{Category: ErrorInvalidRequest, Message: message}
}

func notFound(category ErrorCategory, id string) error {
	return &DomainError{Category: category, ResourceID: id, Message: "resource not found"}
}

func invalidState(message string, cause error) error {
	return &DomainError{Category: ErrorInvalidState, Message: message, Cause: cause}
}

func internalError(cause error) error {
	return &DomainError{Category: ErrorInternal, Message: "internal runtime failure", Cause: cause}
}

func compilationError(message string, cause error) error {
	return &DomainError{Category: ErrorCompilation, Message: message, Cause: cause}
}

func resourceExhausted(message string) error {
	return &DomainError{Category: ErrorResourceExhausted, Message: message}
}

func ignoreMissingResource(err error, category ErrorCategory) error {
	var domain *DomainError
	if errors.As(err, &domain) && domain.Category == category {
		return nil
	}

	return err
}

func failureFromError(category ErrorCategory, err error) *Failure {
	return &Failure{
		Category:    category,
		Message:     "runtime operation failed",
		Diagnostics: diagnosticsFromError(err),
	}
}

func diagnosticsFromError(err error) diagnostics.Diagnostics {
	if err == nil {
		return nil
	}

	var values diagnostics.Diagnostics
	if errors.As(err, &values) {
		return cloneDiagnostics(values)
	}

	var pointer *diagnostics.Diagnostics
	if errors.As(err, &pointer) && pointer != nil {
		return cloneDiagnostics(*pointer)
	}

	return nil
}

func cloneDiagnostics(values diagnostics.Diagnostics) diagnostics.Diagnostics {
	if values == nil {
		return nil
	}

	result := make(diagnostics.Diagnostics, len(values))
	for i, value := range values {
		result[i] = value
		result[i].Annotations = append([]diagnostics.Annotation(nil), value.Annotations...)
	}

	return result
}
