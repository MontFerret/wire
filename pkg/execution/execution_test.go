package execution_test

import (
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestStateTerminal(t *testing.T) {
	tests := []struct {
		state    execution.State
		terminal bool
	}{
		{},
		{state: execution.StateRunning},
		{state: execution.StateCompleted, terminal: true},
		{state: execution.StateFailed, terminal: true},
		{state: execution.StateCancelled, terminal: true},
	}

	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("State(%d).Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}

func TestExecutionSemanticTypesUseUnifiedOutputAndSharedFailure(t *testing.T) {
	output := &api.Output{ContentType: "application/json", Content: []byte("1")}
	terminalFailure := &failure.Failure{Category: failure.CategoryExecution, Message: "runtime operation failed"}
	event := execution.Event{
		Sequence: 7,
		Snapshot: execution.Snapshot{
			State:   execution.StateFailed,
			Output:  output,
			Failure: terminalFailure,
		},
	}

	if event.Sequence != 7 || event.Snapshot.Output != output || event.Snapshot.Failure != terminalFailure {
		t.Fatalf("unexpected execution event: %#v", event)
	}
}
