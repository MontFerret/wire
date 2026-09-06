package core

import "github.com/MontFerret/wire/pkg/failure"

type (
	ErrorKind uint8

	DomainError struct {
		Kind       ErrorKind
		ResourceID string
		Message    string
		Cause      error
	}
)

const (
	ErrorKindInvalidRequest ErrorKind = iota + 1
	ErrorKindCompilation
	ErrorKindExecution
	ErrorKindPlanNotFound
	ErrorKindExecutionNotFound
	ErrorKindDebugSessionNotFound
	ErrorKindConnectionNotFound
	ErrorKindInvalidState
	ErrorKindUnsupported
	ErrorKindInternal
	ErrorKindWatcherLagged
	ErrorKindResourceExhausted
	ErrorKindBreakpointNotFound
	ErrorKindSessionNotFound
)

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

// Category returns the shared Wire failure category; transport-native conditions have none.
func (e *DomainError) Category() failure.Category {
	switch e.Kind {
	case ErrorKindCompilation:
		return failure.CategoryCompilation
	case ErrorKindExecution:
		return failure.CategoryExecution
	case ErrorKindPlanNotFound:
		return failure.CategoryPlanNotFound
	case ErrorKindExecutionNotFound:
		return failure.CategoryExecutionNotFound
	case ErrorKindDebugSessionNotFound:
		return failure.CategoryDebugSessionNotFound
	case ErrorKindConnectionNotFound:
		return failure.CategoryConnectionNotFound
	case ErrorKindInvalidState:
		return failure.CategoryInvalidState
	case ErrorKindWatcherLagged:
		return failure.CategoryWatcherLagged
	case ErrorKindBreakpointNotFound:
		return failure.CategoryBreakpointNotFound
	case ErrorKindInternal:
		return failure.CategoryInternalRuntime
	case ErrorKindSessionNotFound:
		return failure.CategorySessionNotFound
	default:
		return 0
	}
}
