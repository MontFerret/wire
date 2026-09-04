package core

import (
	"github.com/MontFerret/api"
	apidebugger "github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
)

type debugSessionState struct {
	status   wiredebugger.State
	reason   apidebugger.Reason
	location *source.Range
	hitIDs   []apidebugger.BreakpointID
	depth    int
	output   *api.Output
	failure  *failure.Failure
}

func (s *debugSessionState) beginRunning() {
	s.status = wiredebugger.StateRunning
	s.reason = ""
	s.location = nil
	s.hitIDs = nil
	s.depth = 0
	s.failure = nil
}

func (s *debugSessionState) terminate() {
	s.status = wiredebugger.StateTerminated
	s.reason = ""
	s.location = nil
	s.depth = 0
	s.failure = nil
}

func (s *debugSessionState) snapshot(id DebugSessionID) DebugSessionRecord {
	return DebugSessionRecord{
		ID: id,
		Snapshot: cloneDebugSnapshot(wiredebugger.Snapshot{
			State:            s.status,
			StopReason:       s.reason,
			Location:         s.location,
			HitBreakpointIDs: s.hitIDs,
			Depth:            s.depth,
			Output:           s.output,
			Failure:          s.failure,
		}),
	}
}
