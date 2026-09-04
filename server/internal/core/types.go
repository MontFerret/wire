package core

import (
	"fmt"
	"reflect"

	wireruntime "github.com/MontFerret/wire/pkg/runtime"
	"github.com/google/uuid"
)

type (
	ConnectionID   string
	PlanID         string
	ExecutionID    string
	DebugSessionID string

	RuntimeInfo struct {
		ProtocolName    string
		ProtocolVersion string
		RuntimeIdentity wireruntime.Identity
	}

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

func validateID[T ~string](value T, name string) error {
	if value == "" {
		return invalidRequest(fmt.Sprintf("%s is required", name))
	}

	if _, err := uuid.Parse(string(value)); err != nil {
		return invalidRequest(fmt.Sprintf("%s is malformed", name))
	}

	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
