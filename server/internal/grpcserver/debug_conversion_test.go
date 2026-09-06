package grpcserver

import (
	"errors"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/server/internal/core"
)

func TestUnifiedDebuggerTypesPreservePortableProtocolFields(t *testing.T) {
	requested := source.Location{Position: source.Position{Line: 3, Column: 1}, SourceName: "debug.fql"}
	resolved := source.Range{
		Location: source.Location{Position: source.Position{Line: 4, Column: 2}, SourceName: "debug.fql"},
		Span:     source.Span{Start: 10, End: 20},
	}

	convertedBreakpoint, err := breakpoint(debugger.Breakpoint{
		Location:          resolved,
		RequestedLocation: requested,
		ID:                7,
		PointID:           8,
		FunctionID:        9,
		BindingMode:       debugger.BreakpointBindExact,
		Bound:             true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if convertedBreakpoint.GetId() != 7 || convertedBreakpoint.GetRequestedLocation().GetSourceName() != "debug.fql" ||
		convertedBreakpoint.GetRequestedLocation().GetPosition().GetLine() != 3 ||
		convertedBreakpoint.GetLocation().GetLocation().GetPosition().GetLine() != 4 ||
		convertedBreakpoint.GetLocation().GetSpan().GetStart() != 10 || convertedBreakpoint.GetLocation().GetSpan().GetEnd() != 20 ||
		convertedBreakpoint.GetPointId() != 8 || convertedBreakpoint.GetFunctionId() != 9 ||
		convertedBreakpoint.GetBindingMode() != wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_EXACT ||
		!convertedBreakpoint.GetBound() {
		t.Fatalf("unexpected breakpoint transport projection: %#v", convertedBreakpoint)
	}

	unboundBreakpoint, err := breakpoint(debugger.Breakpoint{ID: 10, RequestedLocation: requested})
	if err != nil {
		t.Fatal(err)
	}

	if unboundBreakpoint.GetRequestedLocation().GetSourceName() != "debug.fql" || unboundBreakpoint.GetLocation() != nil ||
		unboundBreakpoint.GetBound() ||
		unboundBreakpoint.GetBindingMode() != wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_SOURCE {
		t.Fatalf("unexpected unbound breakpoint transport projection: %#v", unboundBreakpoint)
	}

	convertedFrame, err := frame(debugger.Frame{Name: "main", Location: resolved.Location, FunctionID: 11})
	if err != nil {
		t.Fatal(err)
	}

	if convertedFrame.GetName() != "main" || convertedFrame.GetLocation().GetPosition().GetLine() != 4 ||
		convertedFrame.GetFunctionId() != 11 {
		t.Fatalf("unexpected frame transport projection: %#v", convertedFrame)
	}

	convertedVariable, err := variable(debugger.Variable{
		Name: "input", Value: debugger.Value{Type: "int", Display: "7", Reference: 13}, Mutable: true, Param: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if convertedVariable.GetName() != "input" || convertedVariable.GetValue().GetReference() != 13 ||
		!convertedVariable.GetMutable() || !convertedVariable.GetParameter() {
		t.Fatalf("unexpected variable transport projection: %#v", convertedVariable)
	}

	convertedSession, err := debugSession("", wiredebugger.Snapshot{
		State:            wiredebugger.StateStopped,
		StopReason:       debugger.ReasonBreakpoint,
		Location:         &resolved,
		HitBreakpointIDs: []debugger.BreakpointID{7},
		Depth:            3,
	})
	if err != nil {
		t.Fatal(err)
	}

	if convertedSession.GetStopReason() != wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT ||
		convertedSession.GetLocation().GetLocation().GetPosition().GetLine() != 4 ||
		convertedSession.GetLocation().GetSpan().GetStart() != 10 || convertedSession.GetDepth() != 3 ||
		len(convertedSession.GetHitBreakpointIds()) != 1 || convertedSession.GetHitBreakpointIds()[0] != 7 {
		t.Fatalf("unexpected debug-session transport projection: %#v", convertedSession)
	}
}

func TestBreakpointOptionsMapEveryUnifiedBindingMode(t *testing.T) {
	tests := []struct {
		name    string
		options *wirev1.BreakpointOptions
		want    debugger.BreakpointBindingMode
	}{
		{name: "missing", want: debugger.BreakpointBindNextExecutableInSource},
		{name: "unspecified", options: breakpointProtocolOptions(wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_UNSPECIFIED), want: debugger.BreakpointBindNextExecutableInSource},
		{name: "next in source", options: breakpointProtocolOptions(wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_SOURCE), want: debugger.BreakpointBindNextExecutableInSource},
		{name: "exact", options: breakpointProtocolOptions(wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_EXACT), want: debugger.BreakpointBindExact},
		{name: "next in function", options: breakpointProtocolOptions(wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_FUNCTION), want: debugger.BreakpointBindNextExecutableInFunction},
	}

	for _, test := range tests {
		options, err := breakpointOptions(test.options)
		if err != nil {
			t.Fatal(err)
		}

		if options.BindingMode != test.want {
			t.Fatalf("%s binding mode mapped to %v, want %v", test.name, options.BindingMode, test.want)
		}
	}
}

func breakpointProtocolOptions(mode wirev1.BreakpointBindingMode) *wirev1.BreakpointOptions {
	return &wirev1.BreakpointOptions{BindingMode: mode}
}

func TestUnifiedDebuggerStopReasonsMapToProtocol(t *testing.T) {
	tests := []struct {
		reason debugger.Reason
		want   wirev1.DebugStopReason
	}{
		{want: wirev1.DebugStopReason_DEBUG_STOP_REASON_UNSPECIFIED},
		{reason: debugger.ReasonEntry, want: wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY},
		{reason: debugger.ReasonBreakpoint, want: wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT},
		{reason: debugger.ReasonStep, want: wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP},
		{reason: debugger.ReasonPause, want: wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE},
		{reason: debugger.ReasonRuntimeError, want: wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR},
	}

	for _, test := range tests {
		got, err := debugStopReason(test.reason)
		if err != nil {
			t.Fatal(err)
		}

		if got != test.want {
			t.Fatalf("debugStopReason(%q) = %s, want %s", test.reason, got, test.want)
		}
	}
}

func TestDebuggerBoundaryRejectsMalformedRepresentations(t *testing.T) {
	maxInt := uint64(^uint(0) >> 1)
	requested := source.Location{Position: source.Position{Line: 1}, SourceName: "debug.fql"}
	tests := []struct {
		name     string
		err      error
		category core.ErrorKind
	}{
		{
			name: "zero inbound breakpoint ID",
			err: func() error {
				_, err := debuggerIDFromProto[debugger.BreakpointID](0, "breakpoint ID")

				return err
			}(),
			category: core.ErrorKindInvalidRequest,
		},
		{
			name: "overflowing inbound value reference",
			err: func() error {
				_, err := debuggerIDFromProto[debugger.ValueReference](maxInt+1, "value reference")

				return err
			}(),
			category: core.ErrorKindInvalidRequest,
		},
		{
			name: "invalid inbound binding mode",
			err: func() error {
				_, err := breakpointOptions(&wirev1.BreakpointOptions{BindingMode: wirev1.BreakpointBindingMode(99)})

				return err
			}(),
			category: core.ErrorKindInvalidRequest,
		},
		{
			name: "zero runtime breakpoint ID",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{RequestedLocation: requested})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "negative runtime breakpoint point ID",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{ID: 1, PointID: -1, RequestedLocation: requested})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "missing runtime requested breakpoint location",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{ID: 1})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "missing bound runtime breakpoint location",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{ID: 1, RequestedLocation: requested, Bound: true})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "negative runtime value reference",
			err: func() error {
				_, err := debugValue(debugger.Value{Reference: -1})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "negative runtime frame function ID",
			err: func() error {
				_, err := frame(debugger.Frame{FunctionID: -1})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "runtime location without file",
			err: func() error {
				_, err := sourceLocation(source.Location{Position: source.Position{Line: 1}})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "runtime span end before start",
			err: func() error {
				_, err := sourceRange(source.Range{Location: requested, Span: source.Span{Start: 2, End: 1}})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "non-nil empty debug location",
			err: func() error {
				empty := source.Range{}
				_, err := debugSession("", wiredebugger.Snapshot{
					State: wiredebugger.StateStopped, Location: &empty,
				})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
		{
			name: "negative runtime debug depth",
			err: func() error {
				_, err := debugSession("", wiredebugger.Snapshot{State: wiredebugger.StateStopped, Depth: -1})

				return err
			}(),
			category: core.ErrorKindInternal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var domain *core.DomainError
			if !errors.As(test.err, &domain) || domain.Kind != test.category {
				t.Fatalf("unexpected conversion result: %v", test.err)
			}
		})
	}
}
