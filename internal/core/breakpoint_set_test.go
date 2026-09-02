package core

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

func TestBreakpointSetAddsDeletesAndTracksRuntimeIDs(t *testing.T) {
	runtime := &spyDebugger{}
	set := newBreakpointSet(runtime, 2)
	location := source.Location{File: "query.fql", Position: source.Position{Line: 1}}
	options := debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindExact}

	value, err := set.set(context.Background(), location, options)
	if err != nil {
		t.Fatal(err)
	}
	if value.ID != 1 || value.RequestedLocation != location || value.BindingMode != options.BindingMode {
		t.Fatalf("unexpected breakpoint: %#v", value)
	}

	if err := set.delete(context.Background(), value.ID); err != nil {
		t.Fatal(err)
	}
	if err := set.delete(context.Background(), value.ID); !hasCategory(err, ErrorBreakpointNotFound) {
		t.Fatalf("deleted breakpoint remained registered: %v", err)
	}

	setCalls, deleteCalls := runtime.breakpointCalls()
	if setCalls != 1 || deleteCalls != 1 {
		t.Fatalf("unexpected runtime calls: set=%d delete=%d", setCalls, deleteCalls)
	}
}

func TestBreakpointSetEnforcesLimitBeforeRuntimeMutation(t *testing.T) {
	runtime := &spyDebugger{}
	set := newBreakpointSet(runtime, 1)
	location := source.Location{File: "query.fql", Position: source.Position{Line: 1}}

	if _, err := set.set(context.Background(), location, debugger.BreakpointOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := set.set(context.Background(), location, debugger.BreakpointOptions{}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected breakpoint limit result: %v", err)
	}

	setCalls, _ := runtime.breakpointCalls()
	if setCalls != 1 {
		t.Fatalf("breakpoint limit allowed %d runtime calls", setCalls)
	}
}

func TestBreakpointSetCommitsLocalStateOnlyAfterRuntimeSuccess(t *testing.T) {
	setFailure := errors.New("set failed")
	deleteFailure := errors.New("delete failed")
	runtime := &spyDebugger{
		setBreakpoint: func(location source.Location, options debugger.BreakpointOptions) (debugger.Breakpoint, error) {
			if setFailure != nil {
				return debugger.Breakpoint{}, setFailure
			}

			return debugger.Breakpoint{ID: 7, RequestedLocation: location, BindingMode: options.BindingMode}, nil
		},
		deleteBreakpoint: func(debugger.BreakpointID) error {
			return deleteFailure
		},
	}
	set := newBreakpointSet(runtime, 1)
	location := source.Location{File: "query.fql", Position: source.Position{Line: 1}}

	if _, err := set.set(context.Background(), location, debugger.BreakpointOptions{}); !hasCategory(err, ErrorInvalidState) || !errors.Is(err, setFailure) {
		t.Fatalf("runtime set failure was not propagated: %v", err)
	}

	setFailure = nil
	value, err := set.set(context.Background(), location, debugger.BreakpointOptions{})
	if err != nil {
		t.Fatalf("failed set consumed local capacity: %v", err)
	}

	if err := set.delete(context.Background(), value.ID); !hasCategory(err, ErrorInvalidState) || !errors.Is(err, deleteFailure) {
		t.Fatalf("runtime delete failure was not propagated: %v", err)
	}

	deleteFailure = nil
	if err := set.delete(context.Background(), value.ID); err != nil {
		t.Fatalf("failed delete removed local state: %v", err)
	}
}

func TestDebugSessionRetainsBreakpointValidationAndStateGating(t *testing.T) {
	runtime := &spyDebugger{}
	debugCtx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	session := newDebugSession("session", "owner", "plan", runtime, debugCtx, cancel, 1, 1)
	session.state.status = DebugRunning

	if _, err := session.SetBreakpointAt(context.Background(), source.Location{}, debugger.BreakpointOptions{}); !hasCategory(err, ErrorInvalidRequest) {
		t.Fatalf("request validation no longer precedes state gating: %v", err)
	}

	location := source.Location{File: "query.fql", Position: source.Position{Line: 1}}
	if _, err := session.SetBreakpointAt(context.Background(), location, debugger.BreakpointOptions{}); !hasCategory(err, ErrorInvalidState) {
		t.Fatalf("running session accepted breakpoint mutation: %v", err)
	}

	setCalls, _ := runtime.breakpointCalls()
	if setCalls != 0 {
		t.Fatalf("state-gated breakpoint reached runtime %d times", setCalls)
	}
}
