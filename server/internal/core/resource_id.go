package core

import (
	"fmt"

	"github.com/google/uuid"
)

type (
	ConnectionID   string
	PlanID         string
	SessionID      string
	ExecutionID    string
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
