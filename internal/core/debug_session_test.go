package core

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/internal/panicboundary"
)

func TestDebugSessionRejectsInvalidCommandWithoutRuntimeOrEvent(t *testing.T) {
	runtime := &controllerDebugger{}
	session := newTestCoreDebugSession(t, runtime, 1)
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if _, err := session.Continue(context.Background()); !hasCategory(err, ErrorInvalidState) {
		t.Fatalf("created session accepted continue: %v", err)
	}

	if calls := runtime.snapshotCalls(); len(calls) != 0 {
		t.Fatalf("invalid command reached runtime: %#v", calls)
	}

	select {
	case event := <-subscription.Events:
		t.Fatalf("invalid command published event: %#v", event)
	case <-time.After(25 * time.Millisecond):
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionPublishesAndReplaysCreatedAndRunningSnapshots(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	runtime := &spyDebugger{start: func(context.Context) (*debugger.Event, error) {
		close(entered)
		<-release

		return &debugger.Event{Reason: debugger.ReasonEntry}, nil
	}}
	session := newTestCoreDebugSession(t, runtime, 1)
	created, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	if created.Current.Sequence != 1 || created.Current.Kind != DebugEventCreated ||
		created.Current.Snapshot.State != DebugCreated {
		t.Fatalf("unexpected created snapshot: %#v", created.Current)
	}
	created.Cancel()

	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered

	running, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer running.Cancel()
	if running.Current.Sequence != 2 || running.Current.Kind != DebugEventStarted ||
		running.Current.Snapshot.State != DebugRunning {
		t.Fatalf("unexpected running replay: %#v", running.Current)
	}

	close(release)
	stopped := receiveDebugEvent(t, running.Events)
	if stopped.Sequence != 3 || stopped.Kind != DebugEventStopped || stopped.Snapshot.State != DebugStopped {
		t.Fatalf("unexpected stopped event: %#v", stopped)
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugWatchDisconnectDoesNotCancelSession(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	cancelled := make(chan bool, 1)
	runtime := &spyDebugger{start: func(ctx context.Context) (*debugger.Event, error) {
		close(entered)
		select {
		case <-ctx.Done():
			cancelled <- true

			return nil, ctx.Err()
		case <-release:
			cancelled <- false

			return &debugger.Event{Reason: debugger.ReasonEntry}, nil
		}
	}}
	session := newTestCoreDebugSession(t, runtime, 1)
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered
	_ = receiveDebugEvent(t, subscription.Events)
	subscription.Cancel()

	select {
	case wasCancelled := <-cancelled:
		t.Fatalf("watch disconnect settled runtime command; cancelled=%v", wasCancelled)
	case <-time.After(25 * time.Millisecond):
	}
	if snapshot := session.snapshot(); snapshot.State != DebugRunning {
		t.Fatalf("watch disconnect changed debug state: %#v", snapshot)
	}

	close(release)
	if wasCancelled := <-cancelled; wasCancelled {
		t.Fatal("watch disconnect cancelled the debug operation")
	}
	_ = waitCoreDebugState(t, session, DebugStopped)
	closeTestCoreDebugSession(t, session)
}

func TestDebugValueReferenceValidationUsesCurrentStoppedState(t *testing.T) {
	runtimeErr := errors.New("stale runtime reference")
	var calls atomic.Int32
	runtime := &spyDebugger{variables: func(debugger.ValueReference) ([]debugger.Variable, error) {
		calls.Add(1)

		return nil, runtimeErr
	}}
	session := newTestCoreDebugSession(t, runtime, 1)
	session.state.status = DebugStopped

	if _, err := session.Variables(context.Background(), 0); !hasCategory(err, ErrorInvalidRequest) {
		t.Fatalf("zero reference did not fail as invalid argument: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("zero reference reached the runtime")
	}
	if _, err := session.Variables(context.Background(), 17); !hasCategory(err, ErrorInvalidState) || !errors.Is(err, runtimeErr) {
		t.Fatalf("stale positive reference did not use InvalidState: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("positive reference reached the runtime %d times", calls.Load())
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionRuntimeErrorPreservesPortableDiagnostics(t *testing.T) {
	values := diagnostics.Diagnostics{{
		Kind:    diagnostics.TypeError,
		Message: "invalid expression",
		Source:  source.New("query.fql", "RETURN true + 1"),
		Annotations: []diagnostics.Annotation{{
			Range: source.Range{
				Location: source.Location{Position: source.Position{Line: 1, Column: 7}, SourceName: "query.fql"},
				Span:     source.Span{Start: 7, End: 15},
			},
			Primary: true,
		}},
	}}
	runtime := &spyDebugger{start: func(context.Context) (*debugger.Event, error) {
		return &debugger.Event{Reason: debugger.ReasonRuntimeError, Error: errors.Join(errors.New("secret"), values)}, nil
	}}
	session := newTestCoreDebugSession(t, runtime, 1)
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = receiveDebugEvent(t, subscription.Events)
	stopped := receiveDebugEvent(t, subscription.Events)
	if stopped.Kind != DebugEventStopped || stopped.Snapshot.State != DebugStopped ||
		stopped.Snapshot.Failure == nil || stopped.Snapshot.Failure.Message != "runtime operation failed" ||
		!reflect.DeepEqual(stopped.Snapshot.Failure.Diagnostics, values) {
		t.Fatalf("portable debug diagnostics changed: %#v", stopped)
	}

	values[0].Annotations[0].Message = "mutated"
	if stopped.Snapshot.Failure.Diagnostics[0].Annotations[0].Message == "mutated" {
		t.Fatal("debug snapshot retained runtime diagnostic storage")
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionFailurePreservesPortableDiagnostics(t *testing.T) {
	values := diagnostics.Diagnostics{{
		Kind:    diagnostics.UnexpectedError,
		Message: "debug execution failed",
		Source:  source.New("query.fql", "RETURN 1"),
	}}
	runtime := &spyDebugger{start: func(context.Context) (*debugger.Event, error) {
		return &debugger.Event{Reason: debugger.ReasonTerminated, Error: values}, nil
	}}
	session := newTestCoreDebugSession(t, runtime, 1)
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = receiveDebugEvent(t, subscription.Events)
	failed := receiveDebugEvent(t, subscription.Events)
	if failed.Kind != DebugEventFailed || failed.Snapshot.State != DebugFailed ||
		failed.Snapshot.Failure == nil || !reflect.DeepEqual(failed.Snapshot.Failure.Diagnostics, values) {
		t.Fatalf("portable terminal debug diagnostics changed: %#v", failed)
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionRuntimeFailurePublishesOrderedTerminalState(t *testing.T) {
	runtimeErr := errors.New("runtime command failed")
	runtime := &spyDebugger{start: func(context.Context) (*debugger.Event, error) {
		return nil, runtimeErr
	}}
	session := newTestCoreDebugSession(t, runtime, 1)
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	running, err := session.Start(context.Background())
	if err != nil || running.State != DebugRunning {
		t.Fatalf("unexpected start result: %#v, %v", running, err)
	}

	started := receiveDebugEvent(t, subscription.Events)
	failed := receiveDebugEvent(t, subscription.Events)
	if started.Kind != DebugEventStarted || started.Snapshot.State != DebugRunning ||
		failed.Kind != DebugEventFailed || failed.Snapshot.State != DebugFailed {
		t.Fatalf("unexpected event order: %#v then %#v", started, failed)
	}

	settled := waitCoreDebugState(t, session, DebugFailed)
	if settled.Failure == nil || settled.Failure.Category != ErrorInternal {
		t.Fatalf("runtime failure did not commit sanitized state: %#v", settled)
	}
}

func TestDebugSessionPauseFailurePreservesRunningStateWithoutEvent(t *testing.T) {
	pauseErr := errors.New("pause failed")
	runtime := &spyDebugger{pause: func() error { return pauseErr }}
	session := newTestCoreDebugSession(t, runtime, 1)
	session.state.status = DebugRunning
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if _, err := session.Pause(context.Background()); !hasCategory(err, ErrorInvalidState) || !errors.Is(err, pauseErr) {
		t.Fatalf("runtime pause failure was not propagated: %v", err)
	}

	if snapshot := session.snapshot(); snapshot.State != DebugRunning {
		t.Fatalf("failed pause changed state: %#v", snapshot)
	}

	select {
	case event := <-subscription.Events:
		t.Fatalf("failed pause published event: %#v", event)
	case <-time.After(25 * time.Millisecond):
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionCommandPanicPublishesFailureAndClosesRuntime(t *testing.T) {
	runtime := &controllerDebugger{panicOn: "start"}
	session := newTestCoreDebugSession(t, runtime, 1)
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	running, err := session.Start(context.Background())
	if err != nil || running.State != DebugRunning {
		t.Fatalf("unexpected start result: %#v, %v", running, err)
	}

	started := receiveDebugEvent(t, subscription.Events)
	failed := receiveDebugEvent(t, subscription.Events)
	if started.Kind != DebugEventStarted || failed.Kind != DebugEventFailed || failed.Snapshot.State != DebugFailed {
		t.Fatalf("unexpected panic event order: %#v then %#v", started, failed)
	}

	settled := waitCoreDebugState(t, session, DebugFailed)
	if settled.Failure == nil || settled.Failure.Category != ErrorInternal {
		t.Fatalf("runtime panic did not commit an internal failure: %#v", settled)
	}

	waitControllerCalls(t, runtime, []string{"start", "close"})
	if _, err := session.Continue(context.Background()); !hasCategory(err, ErrorInvalidState) {
		t.Fatalf("poisoned session accepted another command: %v", err)
	}

	if got := runtime.snapshotCalls(); !reflect.DeepEqual(got, []string{"start", "close"}) {
		t.Fatalf("poisoned session reached runtime again: %#v", got)
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionSynchronousPanicPoisonsAndClosesRuntime(t *testing.T) {
	location := source.Location{SourceName: "query.fql", Position: source.Position{Line: 1}}
	tests := []struct {
		name    string
		state   DebugState
		panicOn string
		call    func(*DebugSession) error
	}{
		{
			name:    "pause",
			state:   DebugRunning,
			panicOn: "pause",
			call: func(session *DebugSession) error {
				_, err := session.Pause(context.Background())

				return err
			},
		},
		{
			name:    "breakpoint mutation",
			state:   DebugCreated,
			panicOn: "set-breakpoint",
			call: func(session *DebugSession) error {
				_, err := session.SetBreakpoint(context.Background(), location)

				return err
			},
		},
		{
			name:    "inspection",
			state:   DebugStopped,
			panicOn: "frames",
			call: func(session *DebugSession) error {
				_, err := session.Frames(context.Background())

				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &controllerDebugger{panicOn: test.panicOn}
			session := newTestCoreDebugSession(t, runtime, 1)
			session.state.status = test.state
			subscription, err := session.Watch()
			if err != nil {
				t.Fatal(err)
			}
			defer subscription.Cancel()

			err = test.call(session)
			if !hasCategory(err, ErrorInternal) {
				t.Fatalf("runtime panic did not return an internal error: %v", err)
			}

			var panicErr *panicboundary.Error
			if !errors.As(err, &panicErr) || panicErr.Value != "runtime secret" || len(panicErr.Stack) == 0 {
				t.Fatalf("runtime panic diagnostics were not retained: %v", err)
			}

			failed := receiveDebugEvent(t, subscription.Events)
			if failed.Kind != DebugEventFailed || failed.Snapshot.State != DebugFailed ||
				failed.Snapshot.Failure == nil || failed.Snapshot.Failure.Category != ErrorInternal {
				t.Fatalf("runtime panic did not publish a terminal failure: %#v", failed)
			}

			waitControllerCalls(t, runtime, []string{test.panicOn, "close"})
			if err := test.call(session); !hasCategory(err, ErrorInvalidState) {
				t.Fatalf("poisoned session accepted another operation: %v", err)
			}

			if got := runtime.snapshotCalls(); !reflect.DeepEqual(got, []string{test.panicOn, "close"}) {
				t.Fatalf("poisoned session reached runtime again: %#v", got)
			}

			if len(session.breakpoints.values) != 0 {
				t.Fatalf("panicking operation committed breakpoint bookkeeping: %#v", session.breakpoints.values)
			}

			closeTestCoreDebugSession(t, session)
		})
	}
}

func TestDebugSessionStoppedOperationsSerializeWithoutHoldingStateLock(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var nextID atomic.Uint64
	runtime := &spyDebugger{setBreakpoint: func(
		location source.Location,
		options debugger.BreakpointOptions,
	) (debugger.Breakpoint, error) {
		entered <- struct{}{}
		<-release

		return debugger.Breakpoint{
			ID:                debugger.BreakpointID(nextID.Add(1)),
			RequestedLocation: location,
			BindingMode:       options.BindingMode,
		}, nil
	}}
	session := newTestCoreDebugSession(t, runtime, 2)
	location := source.Location{SourceName: "query.fql", Position: source.Position{Line: 1}}
	results := make(chan error, 2)

	go func() {
		_, err := session.SetBreakpoint(context.Background(), location)
		results <- err
	}()
	<-entered

	snapshotResult := make(chan DebugSnapshot, 1)
	go func() { snapshotResult <- session.snapshot() }()
	select {
	case snapshot := <-snapshotResult:
		if snapshot.State != DebugCreated {
			t.Fatalf("unexpected snapshot during runtime call: %#v", snapshot)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime call retained the debug state lock")
	}

	go func() {
		_, err := session.SetBreakpoint(context.Background(), location)
		results <- err
	}()
	select {
	case <-entered:
		t.Fatal("concurrent stopped-state operations reached the runtime together")
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionPauseCanInterruptRunningCommand(t *testing.T) {
	started := make(chan struct{})
	paused := make(chan struct{})
	runtime := &spyDebugger{
		start: func(context.Context) (*debugger.Event, error) {
			close(started)
			<-paused

			return &debugger.Event{Reason: debugger.ReasonPause}, nil
		},
		pause: func() error {
			close(paused)

			return nil
		},
	}
	session := newTestCoreDebugSession(t, runtime, 1)
	subscription, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if _, err := session.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started

	pauseSnapshot, err := session.Pause(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pauseSnapshot.State != DebugRunning {
		t.Fatalf("pause response observed command completion early: %#v", pauseSnapshot)
	}

	stopped := waitCoreDebugState(t, session, DebugStopped)
	if stopped.StopReason != debugger.ReasonPause || runtime.pauses() != 1 {
		t.Fatalf("pause did not complete through the runtime: %#v", stopped)
	}

	events := []DebugEvent{
		receiveDebugEvent(t, subscription.Events),
		receiveDebugEvent(t, subscription.Events),
	}
	if got := []DebugEventKind{events[0].Kind, events[1].Kind}; !reflect.DeepEqual(got, []DebugEventKind{DebugEventStarted, DebugEventStopped}) {
		t.Fatalf("unexpected pause event order: %#v", got)
	}

	closeTestCoreDebugSession(t, session)
}

func TestDebugSessionCloseReachesRuntimeDuringBlockedStoppedOperation(t *testing.T) {
	operationStarted := make(chan struct{})
	releaseOperation := make(chan struct{})
	closeCalled := make(chan struct{})
	runtime := &spyDebugger{
		setBreakpoint: func(
			location source.Location,
			options debugger.BreakpointOptions,
		) (debugger.Breakpoint, error) {
			close(operationStarted)
			<-releaseOperation

			return debugger.Breakpoint{
				ID:                1,
				RequestedLocation: location,
				BindingMode:       options.BindingMode,
			}, nil
		},
		close: func() error {
			close(closeCalled)

			return nil
		},
	}
	session := newTestCoreDebugSession(t, runtime, 1)
	location := source.Location{SourceName: "query.fql", Position: source.Position{Line: 1}}
	operationResult := make(chan error, 1)
	go func() {
		_, err := session.SetBreakpoint(context.Background(), location)
		operationResult <- err
	}()
	<-operationStarted

	closeResult := make(chan error, 1)
	go func() { closeResult <- session.Close(context.Background()) }()
	select {
	case <-closeCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("close waited behind the stopped-state operation")
	}

	select {
	case err := <-closeResult:
		t.Fatalf("close committed state before the stopped-state operation settled: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(releaseOperation)
	if err := <-operationResult; err != nil {
		t.Fatal(err)
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	if snapshot := session.snapshot(); snapshot.State != DebugTerminated {
		t.Fatalf("close did not commit terminal state: %#v", snapshot)
	}
}

func receiveDebugEvent(t *testing.T, events <-chan DebugEvent) DebugEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for debug event")

		return DebugEvent{}
	}
}

func waitCoreDebugState(t *testing.T, session *DebugSession, state DebugState) DebugSnapshot {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snapshot := session.snapshot()
		if snapshot.State == state {
			return snapshot
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("debug session did not reach state %d", state)

	return DebugSnapshot{}
}

func waitControllerCalls(t *testing.T, runtime *controllerDebugger, want []string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := runtime.snapshotCalls(); reflect.DeepEqual(got, want) {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("runtime calls = %#v, want %#v", runtime.snapshotCalls(), want)
}

func closeTestCoreDebugSession(t *testing.T, session *DebugSession) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
