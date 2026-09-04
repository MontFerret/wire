package core

import (
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type (
	DebugState uint8

	debugSessionState struct {
		status   DebugState
		reason   debugger.Reason
		location source.Range
		hitIDs   []debugger.BreakpointID
		depth    int
		output   *Output
		failure  *Failure
	}
)

const (
	DebugCreated DebugState = iota + 1
	DebugRunning
	DebugStopped
	DebugCompleted
	DebugFailed
	DebugTerminated
)

func (s DebugState) terminal() bool {
	return s == DebugCompleted || s == DebugFailed || s == DebugTerminated
}

func (s *debugSessionState) beginRunning() {
	s.status = DebugRunning
	s.reason = ""
	s.location = source.Range{}
	s.hitIDs = nil
	s.depth = 0
	s.failure = nil
}

func (s *debugSessionState) terminate() {
	s.status = DebugTerminated
	s.reason = ""
	s.location = source.Range{}
	s.depth = 0
	s.failure = nil
}

func (s *debugSessionState) snapshot(id DebugSessionID, planID PlanID) DebugSnapshot {
	result := DebugSnapshot{
		ID:               id,
		PlanID:           planID,
		State:            s.status,
		StopReason:       s.reason,
		Location:         s.location,
		HitBreakpointIDs: append([]debugger.BreakpointID(nil), s.hitIDs...),
		Depth:            s.depth,
	}

	if s.output != nil {
		result.Output = &Output{ContentType: s.output.ContentType, Content: append([]byte(nil), s.output.Content...)}
	}

	if s.failure != nil {
		result.Failure = &Failure{
			Category:    s.failure.Category,
			Message:     s.failure.Message,
			Diagnostics: cloneDiagnostics(s.failure.Diagnostics),
		}
	}

	return result
}
