package client

// ExecutionState describes the lifecycle state in an ExecutionSnapshot.
type ExecutionState uint8

// Execution lifecycle states.
const (
	ExecutionRunning ExecutionState = iota + 1
	ExecutionCompleted
	ExecutionFailed
	ExecutionCancelled
)

// Terminal reports whether the execution has reached a final state.
func (state ExecutionState) Terminal() bool {
	switch state {
	case ExecutionCompleted, ExecutionFailed, ExecutionCancelled:
		return true
	default:
		return false
	}
}
