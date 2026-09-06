package debugger

import (
	"github.com/MontFerret/api"
	apidebugger "github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/pkg/failure"
)

type (
	// State describes the lifecycle state in a debugger Snapshot.
	State uint8

	// EventKind identifies an ordered debug state transition. Started and
	// continued events remain distinct even though both carry a running snapshot.
	EventKind uint8

	// Snapshot is the semantic state published for a remote debug session.
	Snapshot struct {
		State            State
		StopReason       apidebugger.Reason
		Location         *source.Range
		HitBreakpointIDs []apidebugger.BreakpointID
		Depth            int
		Output           *api.Output
		Failure          *failure.Failure
	}

	// Event carries an ordered debug-session snapshot.
	Event struct {
		Sequence uint64
		Kind     EventKind
		Snapshot Snapshot
	}
)

// Debug states distinguish an unstarted session, active commands, stops, and terminal outcomes.
const (
	StateCreated State = iota + 1
	StateRunning
	StateStopped
	StateCompleted
	StateFailed
	StateTerminated
)

// Debug event kinds preserve creation, initial start, resume, stop, and terminal transitions.
const (
	EventStarted EventKind = iota + 1
	EventContinued
	EventStopped
	EventCompleted
	EventFailed
	EventTerminated
	EventCreated
)

// Terminal reports whether the debug session has reached a final state.
func (state State) Terminal() bool {
	switch state {
	case StateCompleted, StateFailed, StateTerminated:
		return true
	default:
		return false
	}
}
