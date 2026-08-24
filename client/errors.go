package client

import (
	"errors"
)

type (
	// Failure is a sanitized terminal execution or debug failure. It implements
	// error so terminal failures remain available through errors.As.
	Failure struct {
		Category    ErrorCategory
		Message     string
		Diagnostics []Diagnostic
	}

	// ErrorCategory is the stable Wire failure category independent of transport
	// status codes.
	ErrorCategory uint8

	// Error is a structured Wire failure. Internal transport causes remain
	// available through Unwrap without exposing protocol resource identifiers.
	Error struct {
		Category    ErrorCategory
		Message     string
		Diagnostics []Diagnostic
		cause       error
	}
)

// Structured Wire error categories.
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
	ErrorCancelled
	ErrorValueReferenceNotFound
	ErrorResourceExhausted
	ErrorBreakpointNotFound
)

var (
	// ErrClosed reports an operation attempted through a closed Client or resource
	// handle. Closing begins when the first Close call commits teardown.
	ErrClosed = errors.New("Wire client or resource is closed")

	// ErrExecutionCancelled reports that a remote execution reached its cancelled
	// terminal state. It is distinct from cancellation of a Wait caller's context.
	ErrExecutionCancelled = errors.New("remote execution was cancelled")
)

// Error returns the sanitized terminal failure message.
func (f *Failure) Error() string {
	if f == nil {
		return ""
	}

	return f.Message
}

// Error returns the sanitized Wire error message.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}

	return e.Message
}

// Unwrap returns the underlying transport error for errors.Is and errors.As.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.cause
}
