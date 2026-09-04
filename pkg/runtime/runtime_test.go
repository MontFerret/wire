package runtime_test

import (
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/pkg/failure"
	wireruntime "github.com/MontFerret/wire/pkg/runtime"
)

func TestStateTerminal(t *testing.T) {
	tests := []struct {
		state    wireruntime.State
		terminal bool
	}{
		{},
		{state: wireruntime.StateRunning},
		{state: wireruntime.StateCompleted, terminal: true},
		{state: wireruntime.StateFailed, terminal: true},
		{state: wireruntime.StateCancelled, terminal: true},
	}

	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("State(%d).Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}

func TestRuntimeSemanticTypesUseUnifiedOutputAndSharedFailure(t *testing.T) {
	output := &api.Output{ContentType: "application/json", Content: []byte("1")}
	terminalFailure := &failure.Failure{Category: failure.CategoryExecution, Message: "runtime operation failed"}
	event := wireruntime.Event{
		Sequence: 7,
		Snapshot: wireruntime.Snapshot{
			State:   wireruntime.StateFailed,
			Output:  output,
			Failure: terminalFailure,
		},
	}
	identity := wireruntime.Identity{Name: "host", Version: "1.0.0", InstanceID: "instance"}

	if event.Sequence != 7 || event.Snapshot.Output != output || event.Snapshot.Failure != terminalFailure {
		t.Fatalf("unexpected runtime event: %#v", event)
	}
	if identity.Name != "host" || identity.Version != "1.0.0" || identity.InstanceID != "instance" {
		t.Fatalf("unexpected runtime identity: %#v", identity)
	}
}
