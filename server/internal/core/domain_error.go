package core

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
