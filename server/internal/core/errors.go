package core

import (
	"errors"

	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/wire/pkg/failure"
)

var ErrWatcherLagged = errors.New("wire watcher lagged")

func invalidRequest(message string) error {
	return &DomainError{Kind: ErrorKindInvalidRequest, Message: message}
}

func notFound(kind ErrorKind, id string) error {
	return &DomainError{Kind: kind, ResourceID: id, Message: "resource not found"}
}

func invalidState(message string, cause error) error {
	return &DomainError{Kind: ErrorKindInvalidState, Message: message, Cause: cause}
}

func internalError(cause error) error {
	return &DomainError{Kind: ErrorKindInternal, Message: "internal runtime failure", Cause: cause}
}

func compilationError(message string, cause error) error {
	return &DomainError{Kind: ErrorKindCompilation, Message: message, Cause: cause}
}

func resourceExhausted(message string) error {
	return &DomainError{Kind: ErrorKindResourceExhausted, Message: message}
}

func ignoreMissingResource(err error, kind ErrorKind) error {
	var domain *DomainError
	if errors.As(err, &domain) && domain.Kind == kind {
		return nil
	}

	return err
}

func failureFromError(category failure.Category, err error) *failure.Failure {
	return &failure.Failure{
		Category:    category,
		Message:     "runtime operation failed",
		Diagnostics: DiagnosticsFromError(err),
	}
}

// DiagnosticsFromError extracts and detaches only canonical diagnostics from an error chain.
func DiagnosticsFromError(err error) diagnostics.Diagnostics {
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
