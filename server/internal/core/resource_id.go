package core

import (
	"fmt"

	"github.com/google/uuid"
)

type (
	// ConnectionID is the opaque registry key for a logical connection.
	ConnectionID string
	// PlanID is the opaque registry key for a compiled plan.
	PlanID string
	// SessionID is the opaque registry key for a durable session.
	SessionID string
	// ExecutionID is the opaque registry key for an execution.
	ExecutionID string
	// DebugSessionID is the opaque registry key for a debug session.
	DebugSessionID string
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
