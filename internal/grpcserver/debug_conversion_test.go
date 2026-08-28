package grpcserver

import (
	"errors"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/internal/core"
)

func TestUnifiedDebuggerTypesConvertToExistingProtocolFields(t *testing.T) {
	requested := source.Location{Position: source.Position{Line: 3, Column: 1}, File: "debug.fql"}
	resolved := source.Range{
		Location: source.Location{Position: source.Position{Line: 4, Column: 2}, File: "debug.fql"},
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
	if convertedBreakpoint.GetId() != 7 || convertedBreakpoint.GetFile() != "debug.fql" ||
		convertedBreakpoint.GetRequestedLine() != 3 || convertedBreakpoint.GetRequestedColumn() != 1 ||
		convertedBreakpoint.GetLine() != 4 || convertedBreakpoint.GetColumn() != 2 || !convertedBreakpoint.GetVerified() {
		t.Fatalf("unexpected breakpoint transport projection: %#v", convertedBreakpoint)
	}
	unboundBreakpoint, err := breakpoint(debugger.Breakpoint{
		ID: 10, RequestedLocation: requested,
	})
	if err != nil {
		t.Fatal(err)
	}
	if unboundBreakpoint.GetFile() != "debug.fql" || unboundBreakpoint.GetRequestedLine() != 3 ||
		unboundBreakpoint.GetVerified() || unboundBreakpoint.GetLine() != 0 {
		t.Fatalf("unexpected unbound breakpoint transport projection: %#v", unboundBreakpoint)
	}

	convertedFrame, err := frame(debugger.Frame{Name: "main", Location: resolved.Location, FunctionID: 11}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if convertedFrame.GetIndex() != 2 || convertedFrame.GetName() != "main" || convertedFrame.GetLocation().GetLine() != 4 {
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

	convertedSession, err := debugSession(core.DebugSnapshot{
		State:            core.DebugStopped,
		StopReason:       debugger.ReasonBreakpoint,
		Location:         resolved,
		HitBreakpointIDs: []debugger.BreakpointID{7},
	})
	if err != nil {
		t.Fatal(err)
	}
	if convertedSession.GetStopReason().String() != "DEBUG_STOP_REASON_BREAKPOINT" ||
		convertedSession.GetLocation().GetLine() != 4 || len(convertedSession.GetHitBreakpointIds()) != 1 ||
		convertedSession.GetHitBreakpointIds()[0] != 7 {
		t.Fatalf("unexpected debug-session transport projection: %#v", convertedSession)
	}
}

func TestUnifiedDebuggerStopReasonsMapToExistingProtocol(t *testing.T) {
	tests := []struct {
		reason debugger.Reason
		want   string
	}{
		{want: "DEBUG_STOP_REASON_UNSPECIFIED"},
		{reason: debugger.ReasonEntry, want: "DEBUG_STOP_REASON_ENTRY"},
		{reason: debugger.ReasonBreakpoint, want: "DEBUG_STOP_REASON_BREAKPOINT"},
		{reason: debugger.ReasonStep, want: "DEBUG_STOP_REASON_STEP"},
		{reason: debugger.ReasonPause, want: "DEBUG_STOP_REASON_PAUSE"},
		{reason: debugger.ReasonRuntimeError, want: "DEBUG_STOP_REASON_RUNTIME_ERROR"},
	}

	for _, test := range tests {
		if got := debugStopReason(test.reason).String(); got != test.want {
			t.Fatalf("debugStopReason(%q) = %s, want %s", test.reason, got, test.want)
		}
	}
}

func TestDebuggerBoundaryRejectsInvalidNumericRepresentations(t *testing.T) {
	maxInt := uint64(^uint(0) >> 1)
	tests := []struct {
		name     string
		err      error
		category core.ErrorCategory
	}{
		{
			name: "zero inbound breakpoint ID",
			err: func() error {
				_, err := debuggerIDFromProto[debugger.BreakpointID](0, "breakpoint ID")

				return err
			}(),
			category: core.ErrorInvalidRequest,
		},
		{
			name: "overflowing inbound value reference",
			err: func() error {
				_, err := debuggerIDFromProto[debugger.ValueReference](maxInt+1, "value reference")

				return err
			}(),
			category: core.ErrorInvalidRequest,
		},
		{
			name: "zero runtime breakpoint ID",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{
					RequestedLocation: source.Location{
						Position: source.Position{Line: 1}, File: "debug.fql",
					},
				})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "negative runtime breakpoint ID",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{
					ID: -1,
					RequestedLocation: source.Location{
						Position: source.Position{Line: 1}, File: "debug.fql",
					},
				})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "negative runtime breakpoint point ID",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{PointID: -1})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "missing runtime requested breakpoint location",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{ID: 1})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "missing bound runtime breakpoint location",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{
					ID: 1,
					RequestedLocation: source.Location{
						Position: source.Position{Line: 1}, File: "debug.fql",
					},
					Bound: true,
				})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "different runtime breakpoint files",
			err: func() error {
				_, err := breakpoint(debugger.Breakpoint{
					ID: 1,
					RequestedLocation: source.Location{
						Position: source.Position{Line: 1}, File: "requested.fql",
					},
					Location: source.Range{Location: source.Location{
						Position: source.Position{Line: 2}, File: "resolved.fql",
					}},
					Bound: true,
				})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "negative runtime value reference",
			err: func() error {
				_, err := debugValue(debugger.Value{Reference: -1})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "negative runtime frame function ID",
			err: func() error {
				_, err := frame(debugger.Frame{FunctionID: -1}, 0)

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "invalid runtime location",
			err: func() error {
				_, err := sourceLocation(source.Location{Position: source.Position{Line: -1}})

				return err
			}(),
			category: core.ErrorInternal,
		},
		{
			name: "unrepresentable runtime location",
			err: func() error {
				_, err := sourceLocation(source.Location{Position: source.Position{Line: int(int64(1<<31-1) + 1)}})

				return err
			}(),
			category: core.ErrorInternal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var domain *core.DomainError
			if !errors.As(test.err, &domain) || domain.Category != test.category {
				t.Fatalf("unexpected conversion result: %v", test.err)
			}
		})
	}
}
