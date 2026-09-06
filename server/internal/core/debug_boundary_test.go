package core

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

func TestDebugSessionContainsPanicsAtEveryHostedMethod(t *testing.T) {
	for _, name := range []string{"start", "continue", "step-over", "step-in", "step-out", "pause", "frames", "frame-locals", "variables", "evaluate-frame", "set-breakpoint", "delete-breakpoint", "close"} {
		t.Run(name, func(t *testing.T) {
			hosted := &boundaryDebugger{panicOn: name}
			session := newTestCoreDebugSession(t, hosted, 2)
			session.state.status = wiredebugger.StateStopped
			ctx := testContext(t)
			var err error
			asynchronous := false
			switch name {
			case "start":
				session.state.status = wiredebugger.StateCreated
				_, err = session.Start(ctx)
				asynchronous = true
			case "continue":
				_, err = session.Continue(ctx)
				asynchronous = true
			case "step-over":
				_, err = session.StepOver(ctx)
				asynchronous = true
			case "step-in":
				_, err = session.StepIn(ctx)
				asynchronous = true
			case "step-out":
				_, err = session.StepOut(ctx)
				asynchronous = true
			case "pause":
				session.state.status = wiredebugger.StateRunning
				_, err = session.Pause(ctx)
			case "frames":
				_, err = session.Frames(ctx)
			case "frame-locals":
				_, err = session.FrameLocals(ctx, 0)
			case "variables":
				_, err = session.Variables(ctx, 1)
			case "evaluate-frame":
				_, err = session.EvaluateFrame(ctx, 0, "value")
			case "set-breakpoint":
				_, err = session.SetBreakpoint(ctx, source.Location{SourceName: "query", Position: source.Position{Line: 1}})
			case "delete-breakpoint":
				session.breakpoints.add(debugger.Breakpoint{ID: 1})
				err = session.DeleteBreakpoint(ctx, 1)
			case "close":
				err = session.Close(ctx)
			}

			if asynchronous {
				if err != nil {
					t.Fatal(err)
				}

				snapshot := waitCoreDebugState(t, session, wiredebugger.StateFailed)
				if snapshot.Failure == nil || snapshot.Failure.Category != failure.CategoryInternalRuntime ||
					strings.Contains(snapshot.Failure.Message, "runtime secret") {
					t.Fatalf("hosted panic was not a sanitized terminal failure: %#v", snapshot)
				}
			} else {
				var contained *panicboundary.Error
				if !errors.As(err, &contained) || contained.Value != "runtime secret" ||
					len(contained.Stack) == 0 || strings.Contains(err.Error(), "runtime secret") {
					t.Fatalf("hosted panic lost its sanitized typed cause: %v", err)
				}
			}

			closeErr := session.Close(ctx)
			if name == "close" {
				var contained *panicboundary.Error
				if !errors.As(closeErr, &contained) {
					t.Fatalf("close lost its retained panic: %v", closeErr)
				}
			} else if closeErr != nil {
				t.Fatal(closeErr)
			}

			if got := hosted.snapshotCalls(); name == "close" {
				if !reflect.DeepEqual(got, []string{"close"}) {
					t.Fatalf("close ran more than once: %v", got)
				}
			} else if !reflect.DeepEqual(got, []string{name, "close"}) {
				t.Fatalf("unexpected hosted calls: %v", got)
			}
		})
	}
}

func TestDebugSessionCloseRetainsOneResultForConcurrentCallers(t *testing.T) {
	closeErr := errors.New("close failed")
	hosted := &boundaryDebugger{err: closeErr}
	session := newTestCoreDebugSession(t, hosted, 1)
	ctx := testContext(t)
	const callers = 16
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			ready.Done()
			<-start
			results <- session.Close(ctx)
		}()
	}

	ready.Wait()
	close(start)
	for range callers {
		select {
		case err := <-results:
			if !errors.Is(err, closeErr) {
				t.Fatalf("close result was not retained: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent close did not settle")
		}
	}

	if got := hosted.snapshotCalls(); !reflect.DeepEqual(got, []string{"close"}) {
		t.Fatalf("hosted close calls: %v", got)
	}
}

func TestDebugSessionInspectionDetachesHostedSlices(t *testing.T) {
	frames := []debugger.Frame{{Name: "frame"}}
	locals := []debugger.Variable{{Name: "local"}}
	variables := []debugger.Variable{{Name: "variable"}}
	hosted := &borrowedInspectionDebugger{frames: frames, locals: locals, values: variables}
	session := newTestCoreDebugSession(t, hosted, 1)
	session.state.status = wiredebugger.StateStopped
	gotFrames, err := session.Frames(testContext(t))
	if err != nil {
		t.Fatal(err)
	}

	gotLocals, err := session.FrameLocals(testContext(t), 0)
	if err != nil {
		t.Fatal(err)
	}

	gotVariables, err := session.Variables(testContext(t), 1)
	if err != nil {
		t.Fatal(err)
	}

	frames[0].Name, locals[0].Name, variables[0].Name = "changed", "changed", "changed"
	if gotFrames[0].Name != "frame" || gotLocals[0].Name != "local" || gotVariables[0].Name != "variable" {
		t.Fatal("inspection retained hosted slice storage")
	}

	closeTestCoreDebugSession(t, session)
}
