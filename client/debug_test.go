package client

import (
	"io"
	"strings"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
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
		state    wiredebugger.State
		terminal bool
	}{
		{state: 0},
		{state: wiredebugger.StateCreated},
		{state: wiredebugger.StateRunning},
		{state: wiredebugger.StateStopped},
		{state: wiredebugger.StateCompleted, terminal: true},
		{state: wiredebugger.StateFailed, terminal: true},
		{state: wiredebugger.StateTerminated, terminal: true},
	}

	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("DebugState(%d).Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}

func TestDebugStateAndEventKindConversionsMapEveryProtocolValue(t *testing.T) {
	states := []struct {
		protocol wirev1.DebugState
		want     wiredebugger.State
	}{
		{protocol: wirev1.DebugState_DEBUG_STATE_CREATED, want: wiredebugger.StateCreated},
		{protocol: wirev1.DebugState_DEBUG_STATE_RUNNING, want: wiredebugger.StateRunning},
		{protocol: wirev1.DebugState_DEBUG_STATE_STOPPED, want: wiredebugger.StateStopped},
		{protocol: wirev1.DebugState_DEBUG_STATE_COMPLETED, want: wiredebugger.StateCompleted},
		{protocol: wirev1.DebugState_DEBUG_STATE_FAILED, want: wiredebugger.StateFailed},
		{protocol: wirev1.DebugState_DEBUG_STATE_TERMINATED, want: wiredebugger.StateTerminated},
	}
	for _, test := range states {
		got, err := convertDebugState(test.protocol)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("convertDebugState(%v) = %v, want %v", test.protocol, got, test.want)
		}
	}

	kinds := []struct {
		protocol wirev1.DebugEventKind
		want     wiredebugger.EventKind
	}{
		{protocol: wirev1.DebugEventKind_DEBUG_EVENT_KIND_STARTED, want: wiredebugger.EventStarted},
		{protocol: wirev1.DebugEventKind_DEBUG_EVENT_KIND_CONTINUED, want: wiredebugger.EventContinued},
		{protocol: wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED, want: wiredebugger.EventStopped},
		{protocol: wirev1.DebugEventKind_DEBUG_EVENT_KIND_COMPLETED, want: wiredebugger.EventCompleted},
		{protocol: wirev1.DebugEventKind_DEBUG_EVENT_KIND_FAILED, want: wiredebugger.EventFailed},
		{protocol: wirev1.DebugEventKind_DEBUG_EVENT_KIND_TERMINATED, want: wiredebugger.EventTerminated},
		{protocol: wirev1.DebugEventKind_DEBUG_EVENT_KIND_CREATED, want: wiredebugger.EventCreated},
	}
	for _, test := range kinds {
		got, err := convertDebugEventKind(test.protocol)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("convertDebugEventKind(%v) = %v, want %v", test.protocol, got, test.want)
		}
	}

	if _, err := convertDebugState(wirev1.DebugState(99)); err == nil {
		t.Fatal("unknown debug state was accepted")
	}
	if _, err := convertDebugEventKind(wirev1.DebugEventKind(99)); err == nil {
		t.Fatal("unknown debug event kind was accepted")
	}
}

func TestDebugEventsDistinguishStartedFromContinued(t *testing.T) {
	session := func() *wirev1.DebugSession {
		return &wirev1.DebugSession{State: wirev1.DebugState_DEBUG_STATE_RUNNING}
	}

	started, err := convertDebugEvent(&wirev1.WatchDebugResponse{
		Sequence: 1,
		Kind:     wirev1.DebugEventKind_DEBUG_EVENT_KIND_STARTED,
		Session:  session(),
	})
	if err != nil {
		t.Fatal(err)
	}
	continued, err := convertDebugEvent(&wirev1.WatchDebugResponse{
		Sequence: 2,
		Kind:     wirev1.DebugEventKind_DEBUG_EVENT_KIND_CONTINUED,
		Session:  session(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if started.Kind != wiredebugger.EventStarted || continued.Kind != wiredebugger.EventContinued ||
		started.Snapshot.State != wiredebugger.StateRunning || continued.Snapshot.State != wiredebugger.StateRunning {
		t.Fatalf("debug transitions lost their distinct kinds: %#v, %#v", started, continued)
	}
}

func TestDebugEventsExposeCreatedSnapshot(t *testing.T) {
	event, err := convertDebugEvent(&wirev1.WatchDebugResponse{
		Sequence: 1,
		Kind:     wirev1.DebugEventKind_DEBUG_EVENT_KIND_CREATED,
		Session:  &wirev1.DebugSession{State: wirev1.DebugState_DEBUG_STATE_CREATED},
	})
	if err != nil {
		t.Fatal(err)
	}

	if event.Sequence != 1 || event.Kind != wiredebugger.EventCreated || event.Snapshot.State != wiredebugger.StateCreated {
		t.Fatalf("created snapshot changed: %#v", event)
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
		Location:   debugTestRange("debug.fql", 4, 2, 12, 18),
		Depth:      3,
		HitBreakpointIds: []uint64{
			7,
		},
	}
	snapshot, err := convertDebugSessionSnapshot(value)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StopReason != debugger.ReasonBreakpoint || snapshot.Location == nil ||
		snapshot.Location.Location != (source.Location{Position: source.Position{Line: 4, Column: 2}, SourceName: "debug.fql"}) ||
		snapshot.Location.Span != (source.Span{Start: 12, End: 18}) || snapshot.Depth != 3 ||
		len(snapshot.HitBreakpointIDs) != 1 || snapshot.HitBreakpointIDs[0] != 7 {
		t.Fatalf("unexpected Unified API snapshot: %#v", snapshot)
	}

	value.Location.Location.SourceName = "changed.fql"
	value.HitBreakpointIds[0] = 99
	if snapshot.Location.Location.SourceName != "debug.fql" || snapshot.HitBreakpointIDs[0] != 7 {
		t.Fatalf("debug snapshot retained protobuf storage: %#v", snapshot)
	}

	breakpoint, err := convertBreakpoint(&wirev1.Breakpoint{
		Id:                9,
		RequestedLocation: debugTestLocation("debug.fql", 3, 1),
		Location:          debugTestRange("debug.fql", 4, 2, 12, 18),
		PointId:           10,
		FunctionId:        11,
		BindingMode:       wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_EXACT,
		Bound:             true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if breakpoint.ID != 9 || !breakpoint.Bound || breakpoint.RequestedLocation.Line != 3 || breakpoint.Location.Line != 4 ||
		breakpoint.Location.Span != (source.Span{Start: 12, End: 18}) || breakpoint.PointID != 10 ||
		breakpoint.FunctionID != 11 || breakpoint.BindingMode != debugger.BreakpointBindExact {
		t.Fatalf("unexpected Unified API breakpoint: %#v", breakpoint)
	}

	frame, err := convertFrame(&wirev1.Frame{
		Name: "main", Location: debugTestLocation("debug.fql", 4, 2), FunctionId: 12,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Name != "main" || frame.Location.Line != 4 || frame.FunctionID != 12 {
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
				_, err := convertBreakpoint(&wirev1.Breakpoint{RequestedLocation: debugTestLocation("debug.fql", 1, 0)})

				return err
			},
		},
		{
			name: "overflowing breakpoint ID",
			convert: func() error {
				_, err := convertBreakpoint(&wirev1.Breakpoint{Id: maxInt + 1, RequestedLocation: debugTestLocation("debug.fql", 1, 0)})

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
				_, err := convertSourceLocation(debugTestLocation("debug.fql", -1, 0))

				return err
			},
		},
		{
			name: "unknown breakpoint binding mode",
			convert: func() error {
				_, err := convertBreakpoint(&wirev1.Breakpoint{
					Id: 1, RequestedLocation: debugTestLocation("debug.fql", 1, 0), BindingMode: wirev1.BreakpointBindingMode(99),
				})

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
					Id: 1, RequestedLocation: debugTestLocation("debug.fql", 1, 0),
					Location: debugTestRange("debug.fql", -1, 0, 0, 0), Bound: true,
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
			Kind: wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED,
			Session: &wirev1.DebugSession{
				State: wirev1.DebugState_DEBUG_STATE_STOPPED,
				HitBreakpointIds: []uint64{
					uint64(^uint(0)>>1) + 1,
				},
			},
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

func debugTestLocation(file string, line int64, column int64) *wirev1.Location {
	return &wirev1.Location{SourceName: file, Position: &wirev1.Position{Line: line, Column: column}}
}

func debugTestRange(file string, line int64, column int64, start int64, end int64) *wirev1.Range {
	return &wirev1.Range{
		Location: debugTestLocation(file, line, column),
		Span:     &wirev1.Span{Start: start, End: end},
	}
}
