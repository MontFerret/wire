package client

import (
	"context"
	"slices"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestDebugTransportDistinguishesStartedFromContinued(t *testing.T) {
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

func TestDebugTransportBuildsRequestsAndConvertsResponses(t *testing.T) {
	implementation := &handleServer{}
	connection := startHandleServer(t, implementation)
	transport := newDebugTransport(connection, &session{id: "connection-1"})
	ctx := testClientContext(t)

	id, err := transport.open(
		ctx,
		"plan-1",
		Parameters{"input": int64(1)},
		DebugSessionOptions{OutputContentType: "application/json"},
	)
	if err != nil || id != "debug-connection-1" {
		t.Fatalf("open result = %q, %v", id, err)
	}

	commands := []struct {
		name string
		run  func(context.Context, string) error
	}{
		{name: "start", run: transport.start},
		{name: "continue", run: transport.continueExecution},
		{name: "pause", run: transport.pause},
		{name: "next", run: transport.next},
		{name: "step", run: transport.step},
		{name: "out", run: transport.out},
		{name: "stop", run: transport.stop},
	}
	for _, command := range commands {
		if err := command.run(ctx, id); err != nil {
			t.Fatalf("%s failed: %v", command.name, err)
		}
	}

	breakpoint, err := transport.setBreakpoint(ctx, id, Location{File: "query.fql", Line: 2, Column: 3})
	if err != nil || breakpoint.ID != 1 || breakpoint.File != "query.fql" || breakpoint.Line != 2 || !breakpoint.Verified {
		t.Fatalf("unexpected breakpoint: %#v, %v", breakpoint, err)
	}
	if err := transport.deleteBreakpoint(ctx, id, breakpoint.ID); err != nil {
		t.Fatal(err)
	}

	frames, err := transport.frames(ctx, id)
	if err != nil || len(frames) != 1 || frames[0].Name != "main" {
		t.Fatalf("unexpected frames: %#v, %v", frames, err)
	}
	locals, err := transport.frameLocals(ctx, id, 0)
	if err != nil || len(locals) != 1 || locals[0].Name != "value" || locals[0].Value.Display != "1" {
		t.Fatalf("unexpected locals: %#v, %v", locals, err)
	}
	variables, err := transport.variables(ctx, id, 1)
	if err != nil || len(variables) != 1 || variables[0].Name != "nested" || variables[0].Value.Display != "2" {
		t.Fatalf("unexpected variables: %#v, %v", variables, err)
	}
	value, err := transport.evaluateFrame(ctx, id, 0, "1 + 2")
	if err != nil || value.Display != "3" {
		t.Fatalf("unexpected evaluation: %#v, %v", value, err)
	}

	events, err := transport.watch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.recv()
	if err != nil || event.Kind != DebugEventStopped || event.Snapshot.State != DebugStopped {
		t.Fatalf("unexpected debug event: %#v, %v", event, err)
	}
	if err := transport.release(ctx, id); err != nil {
		t.Fatal(err)
	}

	implementation.mu.Lock()
	openRequest := implementation.openDebugRequest
	implementation.mu.Unlock()
	if openRequest.GetConnectionId().GetValue() != "connection-1" || openRequest.GetPlanId().GetValue() != "plan-1" ||
		openRequest.GetParameters().GetValues()["input"].GetIntegerValue() != 1 || openRequest.GetOutputContentType() != "application/json" {
		t.Fatalf("unexpected open request: %#v", openRequest)
	}

	want := []string{call("new-debug", "connection-1", "plan-1")}
	for _, name := range []string{
		"start", "continue", "pause", "next", "step", "out", "stop", "set-breakpoint", "delete-breakpoint",
		"frames", "frame-locals", "variables", "evaluate", "watch-debug", "release-debug",
	} {
		want = append(want, call(name, "connection-1", id))
	}
	if calls := implementation.recordedCalls(); !slices.Equal(calls, want) {
		t.Fatalf("unexpected debug transport calls: %v", calls)
	}
}
