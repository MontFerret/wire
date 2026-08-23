package core

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type (
	ConnectionID   string
	PlanID         string
	ExecutionID    string
	DebugSessionID string

	RuntimeIdentity struct {
		Name       string
		Version    string
		InstanceID string
	}

	RuntimeInfo struct {
		APIIdentity     string
		WireVersion     string
		FerretVersion   string
		RuntimeIdentity RuntimeIdentity
	}

	DiagnosticSpan struct {
		Start   uint64
		End     uint64
		Label   string
		Primary bool
	}

	Diagnostic struct {
		Kind           string
		Message        string
		Hint           string
		Note           string
		SourceIdentity string
		Spans          []DiagnosticSpan
	}

	ErrorCategory uint8

	DomainError struct {
		Category    ErrorCategory
		ResourceID  string
		Message     string
		Diagnostics []Diagnostic
		Cause       error
	}
)

const (
	ErrorInvalidRequest ErrorCategory = iota + 1
	ErrorCompilation
	ErrorExecution
	ErrorPlanNotFound
	ErrorExecutionNotFound
	ErrorDebugSessionNotFound
	ErrorConnectionNotFound
	ErrorInvalidState
	ErrorUnsupported
	ErrorInternal
	ErrorWatcherLagged
	ErrorValueReferenceNotFound
)

var ErrWatcherLagged = errors.New("wire watcher lagged")

func (e *DomainError) Error() string {
	if e == nil {
		return ""
	}

	if e.Message != "" {
		return e.Message
	}

	return "Ferret Wire operation failed"
}

func (e *DomainError) Unwrap() error {
	return e.Cause
}

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

func validateID[T ~string](value T, name string) error {
	if value == "" {
		return invalidRequest(fmt.Sprintf("%s is required", name))
	}
	if _, err := uuid.Parse(string(value)); err != nil {
		return invalidRequest(fmt.Sprintf("%s is malformed", name))
	}

	return nil
}
