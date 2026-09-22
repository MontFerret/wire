package ferret_test

import (
	"reflect"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

func TestDebuggerRoundTrip(t *testing.T) {
	h := newHarness(t)
	// Keep both calls out of tail position so all three frames can be inspected.
	src := api.NewSource("debug.fql", `LET marker = 10
FUNC outer(marker) {
  FUNC inner(marker) {
    RETURN marker
  }

  LET result = inner(30)
  RETURN result + marker
}

RETURN outer(20) + marker`)

	plan, err := h.runtime.CompileDebug(h.ctx, src, api.WithOptimizationLevel(api.OptimizationNone))
	if err != nil {
		t.Fatal(err)
	}

	h.own(plan)

	session, err := plan.NewDebugSession(h.ctx, api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	h.own(session)

	initial, err := session.ReplaceBreakpoints(h.ctx, src.Name, []debugger.BreakpointRequest{
		{Position: source.Position{Line: 8, Column: 1}},
	})
	if err != nil || len(initial) != 1 || !initial[0].Bound {
		t.Fatalf("initial replacement = %+v, %v", initial, err)
	}

	position := source.Position{Line: 4, Column: 1}

	replacement, err := session.ReplaceBreakpoints(h.ctx, src.Name, []debugger.BreakpointRequest{
		{Position: position},
	})
	if err != nil || len(replacement) != 1 {
		t.Fatalf("replacement = %+v, %v", replacement, err)
	}

	breakpoint := replacement[0]
	if !breakpoint.Bound || breakpoint.ID == initial[0].ID ||
		breakpoint.RequestedLocation != (source.Location{SourceName: src.Name, Position: position}) ||
		breakpoint.Location.SourceName != src.Name || breakpoint.Location.Line != 4 ||
		breakpoint.Location.Column < 1 || breakpoint.Location.Span.End <= breakpoint.Location.Span.Start {
		t.Fatalf("replacement did not bind native inner return: %+v", breakpoint)
	}

	listed, err := session.Breakpoints(h.ctx)
	if err != nil || !reflect.DeepEqual(listed, replacement) {
		t.Fatalf("enumerated breakpoints = %+v, %v; want %+v", listed, err, replacement)
	}

	entry, err := session.Start(h.ctx)
	if err != nil || entry == nil || entry.Reason != debugger.ReasonEntry || entry.Error != nil || entry.Location.SourceName != src.Name {
		t.Fatalf("Start = %+v, %v", entry, err)
	}

	stopped, err := session.Continue(h.ctx)
	if err != nil || stopped == nil || stopped.Reason != debugger.ReasonBreakpoint || stopped.Error != nil ||
		stopped.Location != breakpoint.Location || !reflect.DeepEqual(stopped.HitBreakpointIDs, []debugger.BreakpointID{breakpoint.ID}) {
		t.Fatalf("breakpoint stop = %+v, %v", stopped, err)
	}

	frames, err := session.Frames(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	names := []string{"inner", "outer", "<main>"}
	displays := []string{"30", "20", "10"}

	if len(frames) != len(names) {
		t.Fatalf("Frames = %+v; want three frames", frames)
	}

	for index, frame := range frames {
		if frame.Name != names[index] || frame.Location.SourceName != src.Name {
			t.Fatalf("frame %d = %+v; want %s in %s", index, frame, names[index], src.Name)
		}

		locals, err := session.FrameLocals(h.ctx, index)
		if err != nil {
			t.Fatal(err)
		}

		var marker debugger.Value

		for _, local := range locals {
			if local.Name == "marker" && !local.Param {
				marker = local.Value

				break
			}
		}

		if marker.Display != displays[index] {
			t.Fatalf("frame %d locals = %+v; want marker %s", index, locals, displays[index])
		}

		value, err := session.EvaluateFrame(h.ctx, index, "marker")
		if err != nil || value != marker {
			t.Fatalf("frame %d evaluation = %+v, %v; want %+v", index, value, err, marker)
		}
	}

	completed, err := session.Continue(h.ctx)
	if err != nil || completed == nil || completed.Reason != debugger.ReasonCompleted || completed.Error != nil {
		t.Fatalf("completion = %+v, %v", completed, err)
	}

	requireJSONOutput(t, completed.Output, "60")
}
