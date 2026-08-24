package client

import (
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
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

func TestDebugEventsDistinguishStartedFromContinued(t *testing.T) {
	session := func() *wirev1.DebugSession {
		return &wirev1.DebugSession{State: wirev1.DebugState_DEBUG_STATE_RUNNING}
	}

	started := convertDebugEvent(&wirev1.WatchDebugResponse{
		Sequence: 1,
		Payload: &wirev1.WatchDebugResponse_Started{
			Started: &wirev1.DebugStarted{Session: session()},
		},
	})
	continued := convertDebugEvent(&wirev1.WatchDebugResponse{
		Sequence: 2,
		Payload: &wirev1.WatchDebugResponse_Continued{
			Continued: &wirev1.DebugContinued{Session: session()},
		},
	})

	if started.Kind != DebugEventStarted || continued.Kind != DebugEventContinued ||
		started.Snapshot.State != DebugRunning || continued.Snapshot.State != DebugRunning {
		t.Fatalf("debug transitions lost their distinct kinds: %#v, %#v", started, continued)
	}
}
