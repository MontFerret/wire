package core

import (
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

func TestDebugSessionStateBuildsDefensiveSnapshots(t *testing.T) {
	state := debugSessionState{
		status:   DebugStopped,
		reason:   debugger.ReasonBreakpoint,
		location: source.Range{Location: source.Location{SourceName: "query.fql"}},
		hitIDs:   []debugger.BreakpointID{1},
		depth:    2,
		output:   &Output{ContentType: "application/json", Content: []byte("1")},
		failure:  &Failure{Category: ErrorExecution, Message: "runtime operation failed"},
	}

	snapshot := state.snapshot("session", "plan")
	state.hitIDs[0] = 2
	state.output.Content[0] = '2'
	if snapshot.HitBreakpointIDs[0] != 1 || string(snapshot.Output.Content) != "1" {
		t.Fatalf("snapshot retained live state storage: %#v", snapshot)
	}

	snapshot.HitBreakpointIDs[0] = 3
	snapshot.Output.Content[0] = '3'

	retained := state.snapshot("session", "plan")
	if retained.ID != "session" || retained.PlanID != "plan" || retained.State != DebugStopped ||
		retained.StopReason != debugger.ReasonBreakpoint || retained.Location != state.location || retained.Depth != 2 {
		t.Fatalf("unexpected debug snapshot: %#v", retained)
	}
	if retained.HitBreakpointIDs[0] != 2 || string(retained.Output.Content) != "2" {
		t.Fatalf("snapshot did not own mutable values: %#v", retained)
	}
}

func TestDebugSessionStateTransitionsPreserveSupportingValues(t *testing.T) {
	state := debugSessionState{
		status:   DebugStopped,
		reason:   debugger.ReasonBreakpoint,
		location: source.Range{Location: source.Location{SourceName: "query.fql"}},
		hitIDs:   []debugger.BreakpointID{1},
		depth:    2,
		output:   &Output{Content: []byte("1")},
		failure:  &Failure{Category: ErrorExecution},
	}

	state.beginRunning()
	if state.status != DebugRunning || state.reason != "" || state.location != (source.Range{}) ||
		state.hitIDs != nil || state.depth != 0 || state.failure != nil || state.output == nil {
		t.Fatalf("unexpected running state: %#v", state)
	}

	state.hitIDs = []debugger.BreakpointID{2}
	state.failure = &Failure{Category: ErrorExecution}
	state.terminate()
	if state.status != DebugTerminated || state.reason != "" || state.location != (source.Range{}) ||
		state.depth != 0 || state.failure != nil || len(state.hitIDs) != 1 || state.output == nil {
		t.Fatalf("unexpected terminated state: %#v", state)
	}
}
