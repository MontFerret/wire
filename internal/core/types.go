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

	Limits struct {
		MaxConnections                int
		MaxPlansPerConnection         int
		MaxExecutionsPerConnection    int
		MaxDebugSessionsPerConnection int
		MaxWatchersPerResource        int
		MaxBreakpointsPerDebugSession int
	}

	ErrorCategory uint8

	DomainError struct {
		Category   ErrorCategory
		ResourceID string
		Message    string
		Cause      error
	}
)

func (limits Limits) validate() error {
	values := []int{
		limits.MaxConnections,
		limits.MaxPlansPerConnection,
		limits.MaxExecutionsPerConnection,
		limits.MaxDebugSessionsPerConnection,
		limits.MaxWatchersPerResource,
		limits.MaxBreakpointsPerDebugSession,
	}

	for _, value := range values {
		if value <= 0 {
			return invalidRequest("runtime limits must be positive")
		}
	}

	return nil
}

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
	ErrorResourceExhausted
	ErrorBreakpointNotFound
)

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
