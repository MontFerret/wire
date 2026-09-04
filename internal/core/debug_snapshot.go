package core

import (
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type DebugSnapshot struct {
	ID               DebugSessionID
	PlanID           PlanID
	State            DebugState
	StopReason       debugger.Reason
	Location         source.Range
	HitBreakpointIDs []debugger.BreakpointID
	Depth            int
	Output           *Output
	Failure          *Failure
}

func (s DebugSnapshot) clone() DebugSnapshot {
	result := s
	result.HitBreakpointIDs = append([]debugger.BreakpointID(nil), s.HitBreakpointIDs...)

	if s.Output != nil {
		result.Output = &Output{ContentType: s.Output.ContentType, Content: append([]byte(nil), s.Output.Content...)}
	}

	if s.Failure != nil {
		result.Failure = &Failure{
			Category:    s.Failure.Category,
			Message:     s.Failure.Message,
			Diagnostics: cloneDiagnostics(s.Failure.Diagnostics),
		}
	}

	return result
}
