package client

import (
	"testing"
)

func TestDebugStateTerminal(t *testing.T) {
	tests := []struct {
		state    DebugState
		terminal bool
	}{
		{state: 0},
		{state: DebugCreated},
		{state: DebugRunning},
		{state: DebugStopped},
		{state: DebugCompleted, terminal: true},
		{state: DebugFailed, terminal: true},
		{state: DebugTerminated, terminal: true},
	}

	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("DebugState(%d).Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}
