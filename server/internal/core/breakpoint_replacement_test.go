package core

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

type replacementDebugger struct {
	spyDebugger
	replace   func(context.Context, string, []debugger.BreakpointRequest) ([]debugger.Breakpoint, error)
	enumerate func(context.Context) ([]debugger.Breakpoint, error)
}

func (d *replacementDebugger) ReplaceBreakpoints(ctx context.Context, name string, requests []debugger.BreakpointRequest) ([]debugger.Breakpoint, error) {
	if d.replace != nil {
		return d.replace(ctx, name, requests)
	}

	return d.spyDebugger.ReplaceBreakpoints(ctx, name, requests)
}

func (d *replacementDebugger) Breakpoints(ctx context.Context) ([]debugger.Breakpoint, error) {
	if d.enumerate != nil {
		return d.enumerate(ctx)
	}

	return d.spyDebugger.Breakpoints(ctx)
}

func TestBreakpointReplacementPublicationCancellation(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			hosted := &replacementDebugger{}
			calls := 0
			hosted.replace = func(ctx context.Context, name string, requests []debugger.BreakpointRequest) ([]debugger.Breakpoint, error) {
				calls++

				if !after {
					cancel()

					return nil, ctx.Err()
				}

				values, err := hosted.spyDebugger.ReplaceBreakpoints(ctx, name, requests)
				cancel()

				return values, err
			}
			session := newTestCoreDebugSession(t, hosted, 2)
			requests := []debugger.BreakpointRequest{{Position: source.Position{Line: 1}}}

			values, err := session.ReplaceBreakpoints(ctx, "query.fql", requests)
			if after {
				if err != nil || len(values) != 1 {
					t.Fatalf("published replacement=%+v %v", values, err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("prepublication cancellation=%v", err)
			}

			listed, err := session.Breakpoints(testContext(t))
			if err != nil {
				t.Fatal(err)
			}

			if (len(listed) == 1) != after || calls != 1 {
				t.Fatalf("publication count=%d calls=%d", len(listed), calls)
			}

			closeTestCoreDebugSession(t, session)
		})
	}
}

func TestBreakpointReplacementEnumerationAndPanicFailures(t *testing.T) {
	for _, panicOn := range []string{"enumeration", "replacement"} {
		t.Run(panicOn, func(t *testing.T) {
			hosted := &replacementDebugger{}

			if panicOn == "enumeration" {
				hosted.enumerate = func(context.Context) ([]debugger.Breakpoint, error) { panic("secret enumeration") }
			} else {
				hosted.replace = func(context.Context, string, []debugger.BreakpointRequest) ([]debugger.Breakpoint, error) {
					panic("secret replacement")
				}
			}

			session := newTestCoreDebugSession(t, hosted, 2)
			if _, err := session.ReplaceBreakpoints(testContext(t), "", nil); !hasCategory(err, ErrorKindInternal) {
				t.Fatalf("panic mapping=%v", err)
			}

			if session.Snapshot().State != wiredebugger.StateFailed {
				t.Fatal("panicked debugger not poisoned")
			}

			closeTestCoreDebugSession(t, session)
		})
	}

	hosted := &replacementDebugger{enumerate: func(context.Context) ([]debugger.Breakpoint, error) { return nil, errors.New("enumeration failed") }}

	session := newTestCoreDebugSession(t, hosted, 2)
	if _, err := session.ReplaceBreakpoints(testContext(t), "", nil); err == nil {
		t.Fatal("enumeration failure ignored")
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugCommandExplicitCloseCancelsAndSettlesRequest(t *testing.T) {
	entered := make(chan struct{})
	hosted := &spyDebugger{start: func(ctx context.Context) (*debugger.Event, error) {
		close(entered)
		<-ctx.Done()

		return nil, ctx.Err()
	}}
	session := newTestCoreDebugSession(t, hosted, 2)
	result := make(chan error, 1)
	go func() {
		value, err := session.RunCommand(testContext(t), true, hosted.Start)
		if err == nil {
			err = value.Error
		}

		result <- err
	}()
	ctx := testContext(t)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("command did not enter")
	}

	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("command result=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("close did not settle command")
	}
}

func TestDebugSnapshotRetainsEventAndSeparateCommandError(t *testing.T) {
	ctx := testContext(t)
	session := newTestCoreDebugSession(t, &spyDebugger{}, 2)
	output := &debugger.Event{Reason: debugger.ReasonCompleted, HitBreakpointIDs: []debugger.BreakpointID{2}}

	result, err := session.RunCommand(ctx, true, func(context.Context) (*debugger.Event, error) { return output, context.DeadlineExceeded })
	if err != nil || result.Event == nil || !errors.Is(result.Error, context.DeadlineExceeded) {
		t.Fatalf("command=%+v %v", result, err)
	}

	snapshot := session.Snapshot()
	if snapshot.CommandResult == nil || !reflect.DeepEqual(snapshot.CommandResult.Event, output) || !errors.Is(snapshot.CommandResult.Error, context.DeadlineExceeded) {
		t.Fatalf("snapshot=%+v", snapshot)
	}

	snapshot.CommandResult.Event.HitBreakpointIDs[0] = 999
	if session.Snapshot().CommandResult.Event.HitBreakpointIDs[0] != 2 {
		t.Fatal("snapshot aliases event")
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugCommandStreamReservationsRemainBounded(t *testing.T) {
	session := newTestCoreDebugSession(t, &spyDebugger{}, 1)

	first, err := session.ReserveCommandStream()
	if err != nil {
		t.Fatal(err)
	}

	second, err := session.ReserveCommandStream()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := session.ReserveCommandStream(); !hasCategory(err, ErrorKindResourceExhausted) {
		t.Fatalf("unbounded command streams: %v", err)
	}

	first()
	first()

	replacement, err := session.ReserveCommandStream()
	if err != nil {
		t.Fatal(err)
	}

	second()
	replacement()
	closeTestCoreDebugSession(t, session)
}
