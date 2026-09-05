package integration_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestDebuggerRoundTrip(t *testing.T) {
	pause := harness.NewBlock(t)
	location := source.Range{Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 2, Column: 3}}, Span: source.Span{Start: 3, End: 8}}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{
		Command: func(ctx context.Context, method string, call int) (*debugger.Event, error) {
			switch method {
			case "Start":
				return &debugger.Event{Reason: debugger.ReasonEntry, Location: location, Depth: 2}, nil
			case "Continue":
				if call == 1 {
					return &debugger.Event{Reason: debugger.ReasonBreakpoint, Location: location, Depth: 3, HitBreakpointIDs: []debugger.BreakpointID{2}}, nil
				}

				if call == 2 {
					if err := pause.Wait(ctx); err != nil {
						return nil, err
					}

					return &debugger.Event{Reason: debugger.ReasonPause, Location: location, Depth: 2}, nil
				}

				return &debugger.Event{Reason: debugger.ReasonCompleted, Output: &api.Output{ContentType: "application/json", Content: []byte(`{"done":true}`)}}, nil
			case "StepOut":
				return &debugger.Event{Reason: debugger.ReasonRuntimeError, Location: location, Depth: 1, Error: errors.New("hosted debugger secret")}, nil
			default:
				return &debugger.Event{Reason: debugger.ReasonStep, Location: location, Depth: 2}, nil
			}
		}, Pause: func() error {
			pause.Release()

			return nil
		},
	}}}))
	plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Name: "debug.fql", Content: "RETURN @input"})
	if err != nil {
		t.Fatal(err)
	}

	session, err := plan.NewDebugSession(h.Context(), api.WithParam("input", int64(7)), api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	var breakpoints []debugger.Breakpoint

	for index, mode := range []debugger.BreakpointBindingMode{debugger.BreakpointBindNextExecutableInSource, debugger.BreakpointBindExact, debugger.BreakpointBindNextExecutableInFunction} {
		requested := source.Location{SourceName: "debug.fql", Position: source.Position{Line: index + 1, Column: 3}}
		var breakpoint debugger.Breakpoint

		if index == 0 {
			breakpoint, err = session.SetBreakpoint(requested)
		} else {
			breakpoint, err = session.SetBreakpointAt(requested, debugger.BreakpointOptions{BindingMode: mode})
		}

		if err != nil {
			t.Fatal(err)
		}

		want := debugger.Breakpoint{ID: debugger.BreakpointID(index + 1), RequestedLocation: requested, Location: source.Range{Location: requested, Span: source.Span{Start: 3, End: 8}}, PointID: debugger.PointID(index + 11), FunctionID: 7, BindingMode: mode, Bound: true}
		if breakpoint != want {
			t.Fatalf("breakpoint=%+v want=%+v", breakpoint, want)
		}

		breakpoints = append(breakpoints, breakpoint)
	}

	if !reflect.DeepEqual(session.Breakpoints(), breakpoints) {
		t.Fatalf("breakpoint snapshot=%+v", session.Breakpoints())
	}

	session.Breakpoints()[0].ID = 999

	if !reflect.DeepEqual(session.Breakpoints(), breakpoints) {
		t.Fatal("breakpoint snapshot not defensive")
	}

	if err := session.DeleteBreakpoint(breakpoints[0].ID); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(session.Breakpoints(), breakpoints[1:]) {
		t.Fatal("breakpoint deletion not reflected")
	}

	entry, err := session.Start(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(entry, &debugger.Event{Reason: debugger.ReasonEntry, Location: location, Depth: 2}) {
		t.Fatalf("entry event changed (including nil Error): %#v", entry)
	}

	stopped, err := session.Continue(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(stopped, &debugger.Event{Reason: debugger.ReasonBreakpoint, Location: location, Depth: 3, HitBreakpointIDs: []debugger.BreakpointID{2}}) {
		t.Fatalf("breakpoint event=%#v", stopped)
	}

	frames, err := session.Frames()
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(frames, []debugger.Frame{
		{Name: "top", Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 2, Column: 3}}, FunctionID: 7},
		{Name: "caller", Location: source.Location{SourceName: "caller.fql", Position: source.Position{Line: 4, Column: 5}}, FunctionID: 6},
	}) {
		t.Fatalf("frames=%+v", frames)
	}

	for index := range 2 {
		var locals []debugger.Variable

		if index == 0 {
			locals, err = session.Locals()
		} else {
			locals, err = session.FrameLocals(index)
		}

		if err != nil {
			t.Fatal(err)
		}

		name := []string{"local-0", "local-1"}[index]
		if !reflect.DeepEqual(locals, []debugger.Variable{{Name: name, Value: debugger.Value{Type: "object", Display: "{...}", Reference: 9}, Mutable: true, Param: index == 1}}) {
			t.Fatalf("locals=%+v", locals)
		}
	}

	variables, err := session.Variables(9)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(variables, []debugger.Variable{{Name: "child", Value: debugger.Value{Type: "string", Display: "value"}}}) {
		t.Fatalf("variables=%+v", variables)
	}

	value, err := session.Evaluate(h.Context(), "local")
	if err != nil || value != (debugger.Value{Type: "string", Display: "frame-0:local"}) {
		t.Fatalf("Evaluate=%+v err=%v", value, err)
	}

	value, err = session.EvaluateFrame(h.Context(), 1, "caller")
	if err != nil || value != (debugger.Value{Type: "string", Display: "frame-1:caller"}) {
		t.Fatalf("EvaluateFrame=%+v err=%v", value, err)
	}

	for _, step := range []func(context.Context) (*debugger.Event, error){session.StepOver, session.StepIn} {
		event, err := step(h.Context())
		if err != nil || !reflect.DeepEqual(event, &debugger.Event{Reason: debugger.ReasonStep, Location: location, Depth: 2}) {
			t.Fatalf("step=%#v err=%v", event, err)
		}
	}

	runtimeError, err := session.StepOut(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	var remoteFailure *failure.Failure
	if runtimeError.Reason != debugger.ReasonRuntimeError || runtimeError.Location != location || runtimeError.Depth != 1 || !errors.As(runtimeError.Error, &remoteFailure) || remoteFailure.Category != failure.CategoryExecution {
		t.Fatalf("runtime-error stop=%#v", runtimeError)
	}

	type commandResult struct {
		event *debugger.Event
		err   error
	}
	continued := make(chan commandResult, 1)
	go func() {
		event, err := session.Continue(h.Context())
		continued <- commandResult{event, err}
	}()
	harness.Await(t, pause.Started)

	if err := session.Pause(); err != nil {
		t.Fatal(err)
	}

	paused := harness.Await(t, continued)
	if paused.err != nil || !reflect.DeepEqual(paused.event, &debugger.Event{Reason: debugger.ReasonPause, Location: location, Depth: 2}) {
		t.Fatalf("pause=%+v", paused)
	}

	completed, err := session.Continue(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(completed, &debugger.Event{Reason: debugger.ReasonCompleted, Output: &api.Output{ContentType: "application/json", Content: []byte(`{"done":true}`)}}) {
		t.Fatalf("completion=%#v", completed)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := session.Continue(h.Context()); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("command after Close=%v", err)
	}

	snapshot := h.RuntimeSpy().Recorder().Snapshot()
	id := snapshot.OfKind("debugger")[0].ID
	var commands []string
	var indices []int
	var expressions []string
	var requests []harness.BreakpointRequest

	for _, call := range snapshot.Calls {
		if call.Resource != id {
			continue
		}

		switch call.Method {
		case "Start", "Continue", "StepOver", "StepIn", "StepOut", "Pause":
			commands = append(commands, call.Method)
		case "FrameLocals", "EvaluateFrame":
			indices = append(indices, call.Index)

			if call.Method == "EvaluateFrame" {
				expressions = append(expressions, call.Argument.(string))
			}
		case "SetBreakpointAt":
			requests = append(requests, call.Argument.(harness.BreakpointRequest))
		case "DeleteBreakpoint":
			if call.Argument != debugger.BreakpointID(1) {
				t.Fatalf("deleted wrong breakpoint: %+v", call)
			}
		case "Variables":
			if call.Argument != debugger.ValueReference(9) {
				t.Fatalf("wrong value reference: %+v", call)
			}
		}
	}

	if !reflect.DeepEqual(commands, []string{"Start", "Continue", "StepOver", "StepIn", "StepOut", "Continue", "Pause", "Continue"}) || !reflect.DeepEqual(indices, []int{0, 1, 0, 1}) || !reflect.DeepEqual(expressions, []string{"local", "caller"}) {
		t.Fatalf("commands=%v indices=%v expressions=%v", commands, indices, expressions)
	}

	if len(requests) != 3 {
		t.Fatalf("breakpoint requests=%v", requests)
	}

	for i, request := range requests {
		if request.Location != breakpoints[i].RequestedLocation || request.Options.BindingMode != breakpoints[i].BindingMode {
			t.Fatalf("breakpoint request changed: %+v", request)
		}
	}

	if snapshot.Count(id, "Close") != 1 {
		t.Fatal("debugger cleanup was not exactly once")
	}
}
