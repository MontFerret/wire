package server_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestRuntimeAdapterDebuggerBridge(t *testing.T) {
	hostedDebugger := newContractDebugger()
	hostedPlan := &contractPlan{newDebugSession: func(context.Context, apiSessionOptions) (debugger.Session, error) {
		return hostedDebugger, nil
	}}
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
		return hostedPlan, nil
	}}
	env := newIntegrationEnv(t, hosted)
	remote, err := client.NewRuntime(testContext(t), env.conn)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := remote.CompileDebug(testContext(t), api.Source{Name: "debug.fql", Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := plan.NewDebugSession(testContext(t), api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	entry, err := session.Start(testContext(t))
	if err != nil {
		t.Fatal(err)
	}

	if entry.Reason != debugger.ReasonEntry || entry.Depth != 2 || entry.Location.SourceName != "debug.fql" {
		t.Fatalf("unexpected entry event: %#v", entry)
	}

	defaultBreakpoint, err := session.SetBreakpoint(source.Location{
		SourceName: "debug.fql",
		Position:   source.Position{Line: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	exactBreakpoint, err := session.SetBreakpointAt(
		source.Location{SourceName: "debug.fql", Position: source.Position{Line: 1, Column: 3}},
		debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindExact},
	)
	if err != nil {
		t.Fatal(err)
	}
	functionBreakpoint, err := session.SetBreakpointAt(
		source.Location{SourceName: "debug.fql", Position: source.Position{Line: 3}},
		debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindNextExecutableInFunction},
	)
	if err != nil {
		t.Fatal(err)
	}

	if defaultBreakpoint.BindingMode != debugger.BreakpointBindNextExecutableInSource ||
		exactBreakpoint.BindingMode != debugger.BreakpointBindExact ||
		functionBreakpoint.BindingMode != debugger.BreakpointBindNextExecutableInFunction {
		t.Fatalf("breakpoint binding modes were not preserved: %#v %#v %#v", defaultBreakpoint, exactBreakpoint, functionBreakpoint)
	}
	breakpoints := session.Breakpoints()
	if len(breakpoints) != 3 || breakpoints[0].ID >= breakpoints[1].ID || breakpoints[1].ID >= breakpoints[2].ID {
		t.Fatalf("breakpoint snapshot was not ID ordered: %#v", breakpoints)
	}
	breakpoints[0].ID = 999
	if session.Breakpoints()[0].ID == 999 {
		t.Fatal("breakpoint snapshot was not defensive")
	}

	if err := session.DeleteBreakpoint(defaultBreakpoint.ID); err != nil {
		t.Fatal(err)
	}

	if got := session.Breakpoints(); len(got) != 2 || got[0].ID != exactBreakpoint.ID || got[1].ID != functionBreakpoint.ID {
		t.Fatalf("breakpoint cache did not track deletion: %#v", got)
	}

	frames, err := session.Frames()
	if err != nil || len(frames) != 2 || frames[0].Name != "top" {
		t.Fatalf("unexpected frames: %#v, %v", frames, err)
	}
	locals, err := session.Locals()
	if err != nil || len(locals) != 1 || locals[0].Name != "local" {
		t.Fatalf("unexpected top-frame locals: %#v, %v", locals, err)
	}

	if _, err := session.FrameLocals(1); err != nil {
		t.Fatal(err)
	}
	variables, err := session.Variables(9)
	if err != nil || len(variables) != 1 || variables[0].Name != "child" {
		t.Fatalf("unexpected variables: %#v, %v", variables, err)
	}
	value, err := session.Evaluate(testContext(t), "local")
	if err != nil || value.Display != "frame-0:local" {
		t.Fatalf("unexpected top-frame evaluation: %#v, %v", value, err)
	}
	value, err = session.EvaluateFrame(testContext(t), 1, "caller")
	if err != nil || value.Display != "frame-1:caller" {
		t.Fatalf("unexpected indexed evaluation: %#v, %v", value, err)
	}

	steppedIn, err := session.StepIn(testContext(t))
	if err != nil || steppedIn.Reason != debugger.ReasonBreakpoint || steppedIn.Depth != 3 ||
		!reflect.DeepEqual(steppedIn.HitBreakpointIDs, []debugger.BreakpointID{exactBreakpoint.ID}) {
		t.Fatalf("step-in breakpoint event was not preserved: %#v, %v", steppedIn, err)
	}
	steppedOver, err := session.StepOver(testContext(t))
	if err != nil || steppedOver.Reason != debugger.ReasonStep {
		t.Fatalf("step-over failed: %#v, %v", steppedOver, err)
	}
	runtimeError, err := session.StepOut(testContext(t))
	var runtimeFailure *failure.Failure
	if err != nil || runtimeError.Reason != debugger.ReasonRuntimeError ||
		!errors.As(runtimeError.Error, &runtimeFailure) || runtimeFailure.Category != failure.CategoryExecution {
		t.Fatalf("runtime-error event was not preserved: %#v, %v", runtimeError, err)
	}

	continued := make(chan struct {
		event *debugger.Event
		err   error
	}, 1)
	go func() {
		event, continueErr := session.Continue(testContext(t))
		continued <- struct {
			event *debugger.Event
			err   error
		}{event: event, err: continueErr}
	}()
	<-hostedDebugger.continueStarted
	if err := session.Pause(); err != nil {
		t.Fatal(err)
	}
	paused := <-continued
	if paused.err != nil || paused.event.Reason != debugger.ReasonPause {
		t.Fatalf("Pause did not interrupt active Continue: %#v, %v", paused.event, paused.err)
	}

	completed, err := session.Continue(testContext(t))
	if err != nil || completed.Reason != debugger.ReasonCompleted || completed.Output == nil ||
		string(completed.Output.Content) != `{"done":true}` {
		t.Fatalf("completion event was not preserved: %#v, %v", completed, err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("debug Close did not retain its result: %v", err)
	}

	hostedDebugger.mu.Lock()
	commands := append([]string(nil), hostedDebugger.commands...)
	frameLocals := append([]int(nil), hostedDebugger.frameLocals...)
	evaluateFrames := append([]int(nil), hostedDebugger.evaluateFrames...)
	closeCalls := hostedDebugger.closeCalls
	hostedDebugger.mu.Unlock()
	sort.Strings(commands)
	if !reflect.DeepEqual(commands, []string{"continue", "continue", "start", "step-in", "step-out", "step-over"}) {
		t.Fatalf("unexpected debugger commands: %#v", commands)
	}

	if !reflect.DeepEqual(frameLocals, []int{0, 1}) || !reflect.DeepEqual(evaluateFrames, []int{0, 1}) {
		t.Fatalf("top-frame bridging failed: locals=%#v evaluate=%#v", frameLocals, evaluateFrames)
	}

	if closeCalls != 1 {
		t.Fatalf("hosted debugger closed %d times", closeCalls)
	}

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
}
