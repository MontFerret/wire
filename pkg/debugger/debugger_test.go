package debugger_test

import (
	"testing"

	"github.com/MontFerret/api"
	apidebugger "github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestStateTerminal(t *testing.T) {
	tests := []struct {
		state    wiredebugger.State
		terminal bool
	}{
		{},
		{state: wiredebugger.StateCreated},
		{state: wiredebugger.StateRunning},
		{state: wiredebugger.StateStopped},
		{state: wiredebugger.StateCompleted, terminal: true},
		{state: wiredebugger.StateFailed, terminal: true},
		{state: wiredebugger.StateTerminated, terminal: true},
	}

	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("State(%d).Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}

func TestEventKindValuesPreserveProtocolOrdering(t *testing.T) {
	kinds := []wiredebugger.EventKind{
		wiredebugger.EventStarted,
		wiredebugger.EventContinued,
		wiredebugger.EventStopped,
		wiredebugger.EventCompleted,
		wiredebugger.EventFailed,
		wiredebugger.EventTerminated,
		wiredebugger.EventCreated,
	}

	for index, kind := range kinds {
		if want := wiredebugger.EventKind(index + 1); kind != want {
			t.Errorf("event kind value = %d, want %d", kind, want)
		}
	}
}

func TestSnapshotUsesCanonicalUnifiedAPITypes(t *testing.T) {
	location := &source.Range{Location: source.Location{SourceName: "query.fql", Position: source.Position{Line: 1}}}
	output := &api.Output{ContentType: "text/plain", Content: []byte("1")}
	terminalFailure := &failure.Failure{Category: failure.CategoryExecution, Message: "runtime operation failed"}
	snapshot := wiredebugger.Snapshot{
		State:            wiredebugger.StateStopped,
		StopReason:       apidebugger.ReasonBreakpoint,
		Location:         location,
		HitBreakpointIDs: []apidebugger.BreakpointID{3},
		Depth:            2,
		Output:           output,
		Failure:          terminalFailure,
	}

	if snapshot.Location != location || snapshot.Output != output || snapshot.Failure != terminalFailure ||
		snapshot.StopReason != apidebugger.ReasonBreakpoint || snapshot.HitBreakpointIDs[0] != 3 || snapshot.Depth != 2 {
		t.Fatalf("unexpected debugger snapshot: %#v", snapshot)
	}
}
