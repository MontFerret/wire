package client

import (
	"io"
	"strings"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
)

type debugResponseStream struct {
	grpc.ClientStream
	response *wirev1.WatchDebugResponse
	err      error
}

func (s *debugResponseStream) Recv() (*wirev1.WatchDebugResponse, error) {
	if s.err != nil {
		return nil, s.err
	}

	if s.response == nil {
		return nil, io.EOF
	}

	response := s.response
	s.response = nil

	return response, nil
}

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

	started, err := convertDebugEvent(&wirev1.WatchDebugResponse{
		Sequence: 1,
		Payload: &wirev1.WatchDebugResponse_Started{
			Started: &wirev1.DebugStarted{Session: session()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	continued, err := convertDebugEvent(&wirev1.WatchDebugResponse{
		Sequence: 2,
		Payload: &wirev1.WatchDebugResponse_Continued{
			Continued: &wirev1.DebugContinued{Session: session()},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if started.Kind != DebugEventStarted || continued.Kind != DebugEventContinued ||
		started.Snapshot.State != DebugRunning || continued.Snapshot.State != DebugRunning {
		t.Fatalf("debug transitions lost their distinct kinds: %#v, %#v", started, continued)
	}
}

func TestDebugStopReasonsUseUnifiedAPIValues(t *testing.T) {
	tests := []struct {
		transport wirev1.DebugStopReason
		want      debugger.Reason
	}{
		{transport: wirev1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED},
		{transport: wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY, want: debugger.ReasonEntry},
		{transport: wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT, want: debugger.ReasonBreakpoint},
		{transport: wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP, want: debugger.ReasonStep},
		{transport: wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE, want: debugger.ReasonPause},
		{transport: wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR, want: debugger.ReasonRuntimeError},
	}

	for _, test := range tests {
		got, err := convertDebugStopReason(test.transport)
		if err != nil {
			t.Fatal(err)
		}

		if got != test.want {
			t.Fatalf("convertDebugStopReason(%v) = %q, want %q", test.transport, got, test.want)
		}
	}
}

func TestDebugConversionsUseUnifiedAPITypesAndPreserveTransportFields(t *testing.T) {
	value := &wirev1.DebugSession{
		State:      wirev1.DebugState_DEBUG_STATE_STOPPED,
		StopReason: wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT,
		Location:   &wirev1.SourceLocation{File: "debug.fql", Line: 4, Column: 2},
		HitBreakpointIds: []uint64{
			7,
		},
	}
	snapshot, err := convertDebugSessionSnapshot(value)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StopReason != debugger.ReasonBreakpoint || snapshot.Location == nil ||
		snapshot.Location.Location != (source.Location{Position: source.Position{Line: 4, Column: 2}, File: "debug.fql"}) ||
		snapshot.Location.Span != (source.Span{}) || len(snapshot.HitBreakpointIDs) != 1 || snapshot.HitBreakpointIDs[0] != 7 {
		t.Fatalf("unexpected Unified API snapshot: %#v", snapshot)
	}

	breakpoint, err := convertBreakpoint(&wirev1.Breakpoint{
		Id: 9, File: "debug.fql", RequestedLine: 3, RequestedColumn: 1,
		Line: 4, Column: 2, Verified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if breakpoint.ID != 9 || !breakpoint.Bound || breakpoint.RequestedLocation.Line != 3 || breakpoint.Location.Line != 4 ||
		breakpoint.Location.Span != (source.Span{}) || breakpoint.PointID != 0 || breakpoint.FunctionID != 0 || breakpoint.BindingMode != 0 {
		t.Fatalf("unexpected Unified API breakpoint: %#v", breakpoint)
	}

	frame, err := convertFrame(&wirev1.Frame{
		Index: 0, Name: "main", Location: &wirev1.SourceLocation{File: "debug.fql", Line: 4, Column: 2},
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Name != "main" || frame.Location.Line != 4 || frame.FunctionID != 0 {
		t.Fatalf("unexpected Unified API frame: %#v", frame)
	}

	variable, err := convertVariable(&wirev1.Variable{
		Name: "input", Value: &wirev1.DebugValue{Type: "int", Display: "7", Reference: 11}, Mutable: true, Parameter: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if variable.Name != "input" || variable.Value.Reference != 11 || !variable.Mutable || !variable.Param {
		t.Fatalf("unexpected Unified API variable: %#v", variable)
	}
}

func TestDebugConversionsRejectMalformedTransportValues(t *testing.T) {
	maxInt := uint64(^uint(0) >> 1)
	tests := []struct {
		name    string
		convert func() error
	}{
		{
			name: "zero breakpoint ID",
			convert: func() error {
				_, err := convertBreakpoint(&wirev1.Breakpoint{File: "debug.fql", RequestedLine: 1})

				return err
			},
		},
		{
			name: "overflowing breakpoint ID",
			convert: func() error {
				_, err := convertBreakpoint(&wirev1.Breakpoint{Id: maxInt + 1, File: "debug.fql", RequestedLine: 1})

				return err
			},
		},
		{
			name: "zero hit breakpoint ID",
			convert: func() error {
				_, err := convertDebugSessionSnapshot(&wirev1.DebugSession{
					State: wirev1.DebugState_DEBUG_STATE_STOPPED, HitBreakpointIds: []uint64{0},
				})

				return err
			},
		},
		{
			name: "unknown stop reason",
			convert: func() error {
				_, err := convertDebugSessionSnapshot(&wirev1.DebugSession{
					State: wirev1.DebugState_DEBUG_STATE_STOPPED, StopReason: wirev1.DebugStopReason(99),
				})

				return err
			},
		},
		{
			name: "overflowing hit breakpoint ID",
			convert: func() error {
				_, err := convertDebugSessionSnapshot(&wirev1.DebugSession{
					State: wirev1.DebugState_DEBUG_STATE_STOPPED, HitBreakpointIds: []uint64{maxInt + 1},
				})

				return err
			},
		},
		{
			name: "overflowing debug value reference",
			convert: func() error {
				_, err := convertDebugValue(&wirev1.DebugValue{Reference: maxInt + 1})

				return err
			},
		},
		{
			name: "unknown debug state",
			convert: func() error {
				_, err := convertDebugSessionSnapshot(&wirev1.DebugSession{State: wirev1.DebugState(99)})

				return err
			},
		},
		{
			name: "negative location",
			convert: func() error {
				_, err := convertSourceLocation(&wirev1.SourceLocation{Line: -1})

				return err
			},
		},
		{
			name: "out-of-order frame index",
			convert: func() error {
				_, err := convertFrame(&wirev1.Frame{Index: 2}, 0)

				return err
			},
		},
		{
			name: "missing variable value",
			convert: func() error {
				_, err := convertVariable(&wirev1.Variable{Name: "missing"})

				return err
			},
		},
		{
			name: "negative resolved breakpoint location",
			convert: func() error {
				_, err := convertBreakpoint(&wirev1.Breakpoint{
					Id: 1, File: "debug.fql", RequestedLine: 1, Line: -1,
				})

				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.convert(); err == nil || !strings.Contains(err.Error(), "invalid debugger response") {
				t.Fatalf("unexpected malformed-response result: %v", err)
			}
		})
	}
}

func TestDebugEventsCancelWatchOnMalformedServerValue(t *testing.T) {
	cancelled := false
	events := &DebugEvents{
		stream: &debugResponseStream{response: &wirev1.WatchDebugResponse{
			Payload: &wirev1.WatchDebugResponse_Stopped{Stopped: &wirev1.DebugStopped{Session: &wirev1.DebugSession{
				State: wirev1.DebugState_DEBUG_STATE_STOPPED,
				HitBreakpointIds: []uint64{
					uint64(^uint(0)>>1) + 1,
				},
			}}},
		}},
		cancel: func() { cancelled = true },
	}

	if _, err := events.Recv(); err == nil || !strings.Contains(err.Error(), "invalid debugger response") {
		t.Fatalf("unexpected malformed watch result: %v", err)
	}

	if !cancelled {
		t.Fatal("malformed debugger response did not terminate its watch")
	}
}
