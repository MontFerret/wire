package core

import (
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestDebugSessionStateBuildsDefensiveSnapshots(t *testing.T) {
	state := debugSessionState{
		status:   wiredebugger.StateStopped,
		reason:   debugger.ReasonBreakpoint,
		location: &source.Range{Location: source.Location{SourceName: "query.fql"}},
		hitIDs:   []debugger.BreakpointID{1},
		depth:    2,
		output:   &api.Output{ContentType: "application/json", Content: []byte("1")},
		failure:  &failure.Failure{Category: failure.CategoryExecution, Message: "runtime operation failed"},
	}

	snapshot := state.snapshot()
	state.hitIDs[0] = 2
	state.output.Content[0] = '2'
	if snapshot.HitBreakpointIDs[0] != 1 || string(snapshot.Output.Content) != "1" {
		t.Fatalf("snapshot retained live state storage: %#v", snapshot)
	}

	snapshot.HitBreakpointIDs[0] = 3
	snapshot.Output.Content[0] = '3'

	retained := state.snapshot()
	if retained.State != wiredebugger.StateStopped || retained.StopReason != debugger.ReasonBreakpoint ||
		retained.Location == nil || *retained.Location != *state.location || retained.Location == state.location || retained.Depth != 2 {
		t.Fatalf("unexpected debug snapshot: %#v", retained)
	}
	if retained.HitBreakpointIDs[0] != 2 || string(retained.Output.Content) != "2" {
		t.Fatalf("snapshot did not own mutable values: %#v", retained)
	}
}

func TestDebugSessionStateTransitionsPreserveSupportingValues(t *testing.T) {
	state := debugSessionState{
		status:   wiredebugger.StateStopped,
		reason:   debugger.ReasonBreakpoint,
		location: &source.Range{Location: source.Location{SourceName: "query.fql"}},
		hitIDs:   []debugger.BreakpointID{1},
		depth:    2,
		output:   &api.Output{Content: []byte("1")},
		failure:  &failure.Failure{Category: failure.CategoryExecution},
	}

	state.beginRunning()
	if state.status != wiredebugger.StateRunning || state.reason != "" || state.location != nil ||
		state.hitIDs != nil || state.depth != 0 || state.failure != nil || state.output == nil {
		t.Fatalf("unexpected running state: %#v", state)
	}

	state.hitIDs = []debugger.BreakpointID{2}
	state.failure = &failure.Failure{Category: failure.CategoryExecution}
	state.terminate()
	if state.status != wiredebugger.StateTerminated || state.reason != "" || state.location != nil ||
		state.depth != 0 || state.failure != nil || len(state.hitIDs) != 1 || state.output == nil {
		t.Fatalf("unexpected terminated state: %#v", state)
	}
}
