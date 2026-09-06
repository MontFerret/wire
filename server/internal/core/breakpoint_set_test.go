package core

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

func TestBreakpointSetOwnsOnlyBookkeepingAndCapacity(t *testing.T) {
	set := newBreakpointSet(1)
	value := debugger.Breakpoint{ID: 7}

	if err := set.checkCapacity(); err != nil {
		t.Fatal(err)
	}

	set.add(value)
	got, err := set.get(value.ID)
	if err != nil || got != value {
		t.Fatalf("unexpected stored breakpoint: %#v, %v", got, err)
	}

	if err := set.checkCapacity(); !hasCategory(err, ErrorKindResourceExhausted) {
		t.Fatalf("breakpoint limit was not enforced: %v", err)
	}

	set.delete(value.ID)
	if _, err := set.get(value.ID); !hasCategory(err, ErrorKindBreakpointNotFound) {
		t.Fatalf("deleted breakpoint remained registered: %v", err)
	}

	if err := set.checkCapacity(); err != nil {
		t.Fatalf("deleted breakpoint retained capacity: %v", err)
	}
}

func TestDebugSessionCommitsBreakpointBookkeepingOnlyAfterRuntimeSuccess(t *testing.T) {
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
	session := newTestCoreDebugSession(t, runtime, 1)
	location := source.Location{SourceName: "query.fql", Position: source.Position{Line: 1}}

	if _, err := session.SetBreakpointAt(context.Background(), location, debugger.BreakpointOptions{}); !hasCategory(err, ErrorKindInvalidState) || !errors.Is(err, setFailure) {
		t.Fatalf("runtime set failure was not propagated: %v", err)
	}

	setFailure = nil
	value, err := session.SetBreakpointAt(context.Background(), location, debugger.BreakpointOptions{})
	if err != nil {
		t.Fatalf("failed set consumed local capacity: %v", err)
	}

	if err := session.DeleteBreakpoint(context.Background(), value.ID); !hasCategory(err, ErrorKindInvalidState) || !errors.Is(err, deleteFailure) {
		t.Fatalf("runtime delete failure was not propagated: %v", err)
	}

	if _, err := session.SetBreakpointAt(context.Background(), location, debugger.BreakpointOptions{}); !hasCategory(err, ErrorKindResourceExhausted) {
		t.Fatalf("failed delete removed local state: %v", err)
	}

	deleteFailure = nil
	if err := session.DeleteBreakpoint(context.Background(), value.ID); err != nil {
		t.Fatalf("failed delete corrupted local state: %v", err)
	}
}

func TestDebugSessionRetainsBreakpointValidationAndStateGating(t *testing.T) {
	runtime := &spyDebugger{}
	session := newTestCoreDebugSession(t, runtime, 1)
	session.state.status = wiredebugger.StateRunning

	if _, err := session.SetBreakpointAt(context.Background(), source.Location{}, debugger.BreakpointOptions{}); !hasCategory(err, ErrorKindInvalidRequest) {
		t.Fatalf("request validation no longer precedes state gating: %v", err)
	}

	location := source.Location{SourceName: "query.fql", Position: source.Position{Line: 1}}
	if _, err := session.SetBreakpointAt(context.Background(), location, debugger.BreakpointOptions{}); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("running session accepted breakpoint mutation: %v", err)
	}

	setCalls, _ := runtime.breakpointCalls()
	if setCalls != 0 {
		t.Fatalf("state-gated breakpoint reached runtime %d times", setCalls)
	}
}

func newTestCoreDebugSession(t *testing.T, runtime debugger.Session, maxBreakpoints int) *DebugSession {
	t.Helper()

	debugCtx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	limits := testLimits().resources()
	limits.Watchers, limits.Breakpoints = 1, maxBreakpoints
	plan := &Plan{store: newResourceStore(debugCtx, limits)}

	return newDebugSession(plan, runtime)
}
