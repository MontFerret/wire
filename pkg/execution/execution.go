package execution

import (
	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/pkg/failure"
)

type (
	// State describes the lifecycle state in an execution Snapshot.
	State uint8

	// Snapshot is the semantic state published for a remote execution.
	Snapshot struct {
		State   State
		Output  *api.Output
		Failure *failure.Failure
	}

	// Event carries an ordered execution snapshot.
	Event struct {
		Sequence uint64
		Snapshot Snapshot
	}
)

const (
	StateRunning State = iota + 1
	StateCompleted
	StateFailed
	StateCancelled
)

// Terminal reports whether the execution has reached a final state.
func (state State) Terminal() bool {
	switch state {
	case StateCompleted, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}
