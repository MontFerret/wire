package core

import (
	"fmt"
	"reflect"

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
		ProtocolName    string
		ProtocolVersion string
		RuntimeIdentity RuntimeIdentity
	}

	ErrorCategory uint8

	DomainError struct {
		Category   ErrorCategory
		ResourceID string
		Message    string
		Cause      error
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
	ErrorResourceExhausted
	ErrorBreakpointNotFound
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
