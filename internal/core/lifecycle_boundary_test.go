package core

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

func TestPendingCompileCountsAgainstLimitAndConnectionCloseWaits(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	plan := &spyPlan{}
	runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		close(started)
		<-release

		return plan, nil
	}}
	limits := testLimits()
	limits.MaxPlansPerConnection = 1
	host, err := NewHost(runtime, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	compileResult := make(chan error, 1)
	go func() {
		_, compileErr := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
		compileResult <- compileErr
	}()
	<-started
	if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 2"}}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("pending compile did not count against limit: %v", err)
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- connection.Close(context.Background()) }()
	select {
	case err := <-closeResult:
		t.Fatalf("connection closed before pending compile settled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-compileResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected compile result: %v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	_, _, closeCalls := plan.snapshot()
	if closeCalls != 1 {
		t.Fatalf("unpublished plan closed %d times", closeCalls)
	}
}

func TestPendingDebugCreationCountsAgainstLimitAndConnectionCloseWaits(t *testing.T) {
	started := make(chan struct{})
	plan := &spyPlan{newDebugSession: func(ctx context.Context, _ sessionOptions) (debugger.Session, error) {
		close(started)
		<-ctx.Done()

		return nil, ctx.Err()
	}}
	limits := testLimits()
	limits.MaxDebugSessionsPerConnection = 1
	host, err := NewHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}

	openResult := make(chan error, 1)
	go func() {
		_, openErr := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
		openResult <- openErr
	}()
	<-started
	if _, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("pending debug creation did not count against limit: %v", err)
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- connection.Close(context.Background()) }()
	if err := <-openResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("pending debug creation was not cancelled: %v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
}

func TestClosingPlanCountsAgainstLimitUntilCleanupSettles(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	firstPlan := &spyPlan{close: func() error {
		close(started)
		<-release

		return nil
	}}
	compileCalls := 0
	runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		compileCalls++
		if compileCalls == 1 {
			return firstPlan, nil
		}

		return &spyPlan{}, nil
	}}
	limits := testLimits()
	limits.MaxPlansPerConnection = 1
	host, err := NewHost(runtime, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}

	releaseResult := make(chan error, 1)
	go func() { releaseResult <- connection.ReleasePlan(context.Background(), compiled.ID) }()
	<-started
	if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 2"}}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("closing plan did not count against limit: %v", err)
	}
	close(release)
	if err := <-releaseResult; err != nil {
		t.Fatal(err)
	}
	if err := connection.ReleasePlan(context.Background(), compiled.ID); !hasCategory(err, ErrorPlanNotFound) {
		t.Fatalf("released plan did not become stale: %v", err)
	}
	if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 2"}}); err != nil {
		t.Fatalf("settled cleanup did not release the plan slot: %v", err)
	}
}

func TestConcurrentPlanReleaseSharesResultAndClosesOnce(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	closeErr := errors.New("plan close failed")
	var startOnce sync.Once
	plan := &spyPlan{close: func() error {
		startOnce.Do(func() { close(started) })
		<-release

		return closeErr
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- connection.ReleasePlan(context.Background(), compiled.ID) }()
	<-started
	go func() { second <- connection.ReleasePlan(context.Background(), compiled.ID) }()
	select {
	case err := <-second:
		t.Fatalf("concurrent release settled before plan close: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for i, result := range []<-chan error{first, second} {
		if err := <-result; !errors.Is(err, closeErr) {
			t.Fatalf("release %d did not receive retained result: %v", i, err)
		}
	}
	_, _, closeCalls := plan.snapshot()
	if closeCalls != 1 {
		t.Fatalf("plan closed %d times", closeCalls)
	}
	if err := connection.ReleasePlan(context.Background(), compiled.ID); !hasCategory(err, ErrorPlanNotFound) {
		t.Fatalf("released plan did not become stale: %v", err)
	}
}

func TestResourceLimitsAndConnectionIsolationRemainWireOwned(t *testing.T) {
	plan := &spyPlan{
		newSession: func(context.Context, sessionOptions) (api.Session, error) {
			return &spySession{run: func(ctx context.Context) (api.Output, error) {
				<-ctx.Done()

				return api.Output{}, ctx.Err()
			}}, nil
		},
		newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
			return &spyDebugger{}, nil
		},
	}
	runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}
	limits := testLimits()
	limits.MaxConnections = 2
	limits.MaxPlansPerConnection = 1
	limits.MaxExecutionsPerConnection = 1
	limits.MaxDebugSessionsPerConnection = 1
	host, err := NewHost(runtime, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	other, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.OpenConnection(); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("connection limit was bypassed: %v", err)
	}
	compiled, err := owner.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 2"}}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("plan limit was bypassed: %v", err)
	}
	execution, err := owner.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("execution limit was bypassed: %v", err)
	}
	debugSession, err := owner.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("debug session limit was bypassed: %v", err)
	}
	if _, err := other.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID}); !hasCategory(err, ErrorPlanNotFound) {
		t.Fatalf("plan crossed connection boundary: %v", err)
	}

	if err := owner.ReleaseDebugSession(testContext(t), debugSession.ID); err != nil {
		t.Fatal(err)
	}
	if err := owner.ReleaseExecution(testContext(t), execution.ID); err != nil {
		t.Fatal(err)
	}
	if err := owner.ReleasePlan(testContext(t), compiled.ID); err != nil {
		t.Fatal(err)
	}
}

func TestPlanClosePanicIsSanitizedAndDoesNotRetainResource(t *testing.T) {
	plan := &spyPlan{close: func() error { panic("plan close secret") }}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.ReleasePlan(testContext(t), compiled.ID); !hasCategory(err, ErrorInternal) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("plan panic was not sanitized: %v", err)
	}
	if err := connection.ReleasePlan(context.Background(), compiled.ID); !hasCategory(err, ErrorPlanNotFound) {
		t.Fatalf("panicking plan remained retained: %v", err)
	}
	_, _, closeCalls := plan.snapshot()
	if closeCalls != 1 {
		t.Fatalf("panicking plan close attempted %d times", closeCalls)
	}
}

func TestConcurrentConnectionCloseSharesResultAndThenBecomesStale(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	plan := &spyPlan{close: func() error {
		close(started)
		<-release

		return nil
	}}
	host, err := NewHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}}); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- host.CloseConnection(context.Background(), connection.ID()) }()
	<-started
	go func() { second <- host.CloseConnection(context.Background(), connection.ID()) }()
	select {
	case err := <-second:
		t.Fatalf("concurrent close settled before cleanup: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for i, result := range []<-chan error{first, second} {
		if err := <-result; err != nil {
			t.Fatalf("connection close %d failed: %v", i, err)
		}
	}
	if err := host.CloseConnection(context.Background(), connection.ID()); !hasCategory(err, ErrorConnectionNotFound) {
		t.Fatalf("closed connection did not become stale: %v", err)
	}
	host.mu.RLock()
	closing := len(host.closing)
	host.mu.RUnlock()
	if closing != 0 {
		t.Fatalf("host retained %d settled connection closes", closing)
	}
}

func TestConnectionCloseCancelsExecutionAndReleasesWireResources(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	var finishOnce sync.Once
	session := &spySession{run: func(ctx context.Context) (api.Output, error) {
		close(started)
		<-ctx.Done()
		finishOnce.Do(func() { close(finished) })

		return api.Output{}, ctx.Err()
	}}
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return session, nil
	}}
	runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}
	host, err := NewHost(runtime, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN BLOCK()"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID}); err != nil {
		t.Fatal(err)
	}
	<-started

	if err := host.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("connection close did not cancel execution")
	}
	runCalls, sessionCloseCalls := session.counts()
	if runCalls != 1 || sessionCloseCalls != 1 {
		t.Fatalf("unexpected session lifecycle: run=%d close=%d", runCalls, sessionCloseCalls)
	}
	_, _, planCloseCalls := plan.snapshot()
	if planCloseCalls != 1 {
		t.Fatalf("connection cleanup closed plan %d times", planCloseCalls)
	}
	_, _, runtimeCloseCalls := runtime.snapshot()
	if runtimeCloseCalls != 0 {
		t.Fatalf("connection cleanup closed borrowed runtime %d times", runtimeCloseCalls)
	}
}

func TestSessionClosePanicIsContainedAndAttemptedOnce(t *testing.T) {
	session := &spySession{
		run: func(context.Context) (api.Output, error) {
			return api.Output{ContentType: "application/json", Content: []byte("1")}, nil
		},
		close: func() error { panic("close secret") },
	}
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return session, nil
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	finished := waitExecution(t, connection, execution.ID)
	if finished.State != ExecutionFailed || finished.Failure == nil || finished.Failure.Category != ErrorInternal {
		t.Fatalf("close panic was not contained: %#v", finished)
	}
	_, closeCalls := session.counts()
	if closeCalls != 1 {
		t.Fatalf("panicking session close attempted %d times", closeCalls)
	}
	if err := connection.ReleaseExecution(testContext(t), execution.ID); err != nil {
		t.Fatal(err)
	}
}

func TestDebugClosePanicTerminatesWatcherAndBecomesStale(t *testing.T) {
	debugSession := &spyDebugger{close: func() error { panic("debug close secret") }}
	plan := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
		return debugSession, nil
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := connection.WatchDebug(opened.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if err := connection.ReleaseDebugSession(testContext(t), opened.ID); !hasCategory(err, ErrorInternal) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("debug close panic was not sanitized: %v", err)
	}
	select {
	case event := <-subscription.Events:
		if event.Kind != DebugEventTerminated {
			t.Fatalf("unexpected terminal event: %#v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("debug close panic stranded watcher")
	}
	if err := connection.ReleaseDebugSession(context.Background(), opened.ID); !hasCategory(err, ErrorDebugSessionNotFound) {
		t.Fatalf("released debug session did not become stale: %v", err)
	}
	if closeCalls := debugSession.closes(); closeCalls != 1 {
		t.Fatalf("panicking debug close attempted %d times", closeCalls)
	}
}

func TestPlanReleaseSettlesChildrenBeforeClosingAPIPlan(t *testing.T) {
	var orderMu sync.Mutex
	var order []string
	record := func(value string) {
		orderMu.Lock()
		order = append(order, value)
		orderMu.Unlock()
	}
	executionStarted := make(chan struct{})
	executionSession := &spySession{
		run: func(ctx context.Context) (api.Output, error) {
			close(executionStarted)
			<-ctx.Done()

			return api.Output{}, ctx.Err()
		},
		close: func() error {
			record("execution")

			return nil
		},
	}
	debugSession := &spyDebugger{close: func() error {
		record("debug")

		return nil
	}}
	plan := &spyPlan{
		newSession: func(context.Context, sessionOptions) (api.Session, error) {
			return executionSession, nil
		},
		newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
			return debugSession, nil
		},
		close: func() error {
			record("plan")

			return nil
		},
	}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	<-executionStarted
	opened, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}

	if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
		t.Fatal(err)
	}
	orderMu.Lock()
	settledOrder := append([]string(nil), order...)
	orderMu.Unlock()
	if !reflect.DeepEqual(settledOrder, []string{"debug", "execution", "plan"}) {
		t.Fatalf("unexpected cleanup order: %#v", settledOrder)
	}
	if err := connection.ReleaseExecution(context.Background(), execution.ID); !hasCategory(err, ErrorExecutionNotFound) {
		t.Fatalf("released execution did not become stale: %v", err)
	}
	if err := connection.ReleaseDebugSession(context.Background(), opened.ID); !hasCategory(err, ErrorDebugSessionNotFound) {
		t.Fatalf("released debug session did not become stale: %v", err)
	}
}

func TestDebugUsesUnifiedSessionAndPreservesWireState(t *testing.T) {
	hitIDs := []debugger.BreakpointID{1}
	debugContent := []byte("7")
	debugSession := &spyDebugger{
		start: func(context.Context) (*debugger.Event, error) {
			return &debugger.Event{
				Reason:           debugger.ReasonBreakpoint,
				HitBreakpointIDs: hitIDs,
				Depth:            4,
				Location: source.Range{Location: source.Location{
					Position: source.Position{Line: 1, Column: 2},
					File:     "debug.fql",
				}, Span: source.Span{Start: 3, End: 8}},
			}, nil
		},
		resume: func(context.Context) (*debugger.Event, error) {
			return &debugger.Event{
				Reason: debugger.ReasonCompleted,
				Output: &api.Output{ContentType: "application/json", Content: debugContent},
			}, nil
		},
		frames: []debugger.Frame{{
			Name:       "main",
			FunctionID: 17,
			Location: source.Location{
				Position: source.Position{Line: 1, Column: 2},
				File:     "debug.fql",
			},
		}},
		locals: []debugger.Variable{{
			Name: "input",
			Value: debugger.Value{
				Type:      "int",
				Display:   "7",
				Reference: 23,
			},
			Mutable: true,
			Param:   true,
		}},
	}
	plan := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
		return debugSession, nil
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Name: "debug.fql", Content: "RETURN 7"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{
		PlanID:            compiled.ID,
		Parameters:        map[string]any{"input": int64(7)},
		OutputContentType: "application/json",
	})
	if err != nil {
		t.Fatal(err)
	}
	requested := source.Location{Position: source.Position{Line: 1, Column: 2}, File: "debug.fql"}
	breakpoint, err := connection.SetBreakpoint(context.Background(), opened.ID, requested)
	if err != nil {
		t.Fatal(err)
	}
	if !breakpoint.Bound || breakpoint.ID != 1 || breakpoint.PointID != 41 || breakpoint.FunctionID != 42 ||
		breakpoint.RequestedLocation != requested || breakpoint.Location.Location != requested || breakpoint.Location.Span != (source.Span{Start: 0, End: 1}) {
		t.Fatalf("core did not preserve API breakpoint: %#v", breakpoint)
	}

	if _, err := connection.StartDebug(context.Background(), opened.ID); err != nil {
		t.Fatal(err)
	}
	stopped := waitDebugState(t, connection, opened.ID, DebugStopped)
	wantRange := source.Range{Location: requested, Span: source.Span{Start: 3, End: 8}}
	if stopped.StopReason != debugger.ReasonBreakpoint || stopped.Location != wantRange || stopped.Depth != 4 ||
		!reflect.DeepEqual(stopped.HitBreakpointIDs, []debugger.BreakpointID{1}) {
		t.Fatalf("unexpected stopped state: %#v", stopped)
	}
	hitIDs[0] = 90
	stopped.HitBreakpointIDs[0] = 91
	retainedStopped := waitDebugState(t, connection, opened.ID, DebugStopped)
	if !reflect.DeepEqual(retainedStopped.HitBreakpointIDs, []debugger.BreakpointID{1}) {
		t.Fatalf("debug snapshot did not retain an owned hit-ID slice: %#v", retainedStopped)
	}
	frames, err := connection.Frames(context.Background(), opened.ID)
	if err != nil || !reflect.DeepEqual(frames, debugSession.frames) {
		t.Fatalf("core did not preserve API frames: %#v, %v", frames, err)
	}
	locals, err := connection.FrameLocals(context.Background(), opened.ID, 0)
	if err != nil || !reflect.DeepEqual(locals, debugSession.locals) {
		t.Fatalf("core did not preserve API variables: %#v, %v", locals, err)
	}
	variables, err := connection.Variables(context.Background(), opened.ID, debugger.ValueReference(23))
	if err != nil || !reflect.DeepEqual(variables, debugSession.locals) {
		t.Fatalf("core did not preserve expanded API variables: %#v, %v", variables, err)
	}
	evaluated, err := connection.EvaluateFrame(context.Background(), opened.ID, 0, "input")
	if err != nil || evaluated != (debugger.Value{Type: "string", Display: "wire"}) {
		t.Fatalf("core did not preserve evaluated API value: %#v, %v", evaluated, err)
	}
	if _, err := connection.ContinueDebug(context.Background(), opened.ID); err != nil {
		t.Fatal(err)
	}
	completed := waitDebugState(t, connection, opened.ID, DebugCompleted)
	if completed.Output == nil || completed.Output.ContentType != "application/json" || string(completed.Output.Content) != "7" {
		t.Fatalf("unexpected debug output: %#v", completed.Output)
	}
	debugContent[0] = '8'
	completed.Output.Content[0] = '9'
	retainedCompleted := waitDebugState(t, connection, opened.ID, DebugCompleted)
	if retainedCompleted.Output == nil || string(retainedCompleted.Output.Content) != "7" {
		t.Fatalf("debug snapshot did not retain owned output bytes: %#v", retainedCompleted.Output)
	}

	_, debugOptions, _ := plan.snapshot()
	if len(debugOptions) != 1 || !reflect.DeepEqual(debugOptions[0].params, map[string]any{"input": int64(7)}) || debugOptions[0].contentType != "application/json" {
		t.Fatalf("unexpected debug session options: %#v", debugOptions)
	}
	if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
		t.Fatal(err)
	}
	if closeCalls := debugSession.closes(); closeCalls != 1 {
		t.Fatalf("completed debug session closed %d times", closeCalls)
	}
}

func TestDebugSessionCloseIsRetainedAcrossTerminalAndRelease(t *testing.T) {
	tests := []struct {
		name       string
		start      func(context.Context) (*debugger.Event, error)
		wantState  DebugState
		closeError error
	}{
		{
			name: "completed",
			start: func(context.Context) (*debugger.Event, error) {
				return &debugger.Event{Reason: debugger.ReasonCompleted}, nil
			},
			wantState: DebugCompleted,
		},
		{
			name: "command failure",
			start: func(context.Context) (*debugger.Event, error) {
				return nil, errors.New("runtime command failed")
			},
			wantState: DebugFailed,
		},
		{
			name: "retained close failure",
			start: func(context.Context) (*debugger.Event, error) {
				return &debugger.Event{Reason: debugger.ReasonCompleted}, nil
			},
			wantState:  DebugCompleted,
			closeError: errors.New("runtime close failed"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtimeDebugger := &spyDebugger{
				start: test.start,
				close: func() error { return test.closeError },
			}
			plan := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
				return runtimeDebugger, nil
			}}
			connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
				return plan, nil
			}})
			compiled, err := connection.Compile(context.Background(), CompileInput{
				Source:     api.Source{Content: "RETURN 1"},
				Debuggable: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			opened, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := connection.StartDebug(context.Background(), opened.ID); err != nil {
				t.Fatal(err)
			}
			waitDebugState(t, connection, opened.ID, test.wantState)

			releaseErr := connection.ReleaseDebugSession(testContext(t), opened.ID)
			if !errors.Is(releaseErr, test.closeError) {
				t.Fatalf("unexpected retained close result: %v", releaseErr)
			}
			if closeCalls := runtimeDebugger.closes(); closeCalls != 1 {
				t.Fatalf("runtime debug session closed %d times", closeCalls)
			}
		})
	}
}

func TestDebugSessionStopAndParentCascadeCloseOnce(t *testing.T) {
	t.Run("explicit stop", func(t *testing.T) {
		runtimeDebugger := &spyDebugger{}
		connection, compiled, opened := openTestDebugSession(t, runtimeDebugger)

		if _, err := connection.StopDebug(testContext(t), opened.ID); err != nil {
			t.Fatal(err)
		}
		if err := connection.ReleaseDebugSession(testContext(t), opened.ID); err != nil {
			t.Fatal(err)
		}
		if closeCalls := runtimeDebugger.closes(); closeCalls != 1 {
			t.Fatalf("stopped runtime debug session closed %d times", closeCalls)
		}
		if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("running cancellation", func(t *testing.T) {
		started := make(chan struct{})
		commandReturned := make(chan struct{})
		runtimeDebugger := &spyDebugger{start: func(ctx context.Context) (*debugger.Event, error) {
			defer close(commandReturned)
			close(started)
			<-ctx.Done()

			return nil, ctx.Err()
		}, close: func() error {
			select {
			case <-commandReturned:
				return nil
			case <-time.After(5 * time.Second):
				return errors.New("debug command did not return before close")
			}
		}}
		connection, compiled, opened := openTestDebugSession(t, runtimeDebugger)
		if _, err := connection.StartDebug(context.Background(), opened.ID); err != nil {
			t.Fatal(err)
		}
		<-started
		if _, err := connection.StopDebug(testContext(t), opened.ID); err != nil {
			t.Fatal(err)
		}
		if err := connection.ReleaseDebugSession(testContext(t), opened.ID); err != nil {
			t.Fatal(err)
		}
		if closeCalls := runtimeDebugger.closes(); closeCalls != 1 {
			t.Fatalf("cancelled runtime debug session closed %d times", closeCalls)
		}
		if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("plan cascade", func(t *testing.T) {
		runtimeDebugger := &spyDebugger{}
		connection, compiled, _ := openTestDebugSession(t, runtimeDebugger)

		if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
			t.Fatal(err)
		}
		if closeCalls := runtimeDebugger.closes(); closeCalls != 1 {
			t.Fatalf("cascaded runtime debug session closed %d times", closeCalls)
		}
	})
}

func openTestDebugSession(t *testing.T, runtimeDebugger debugger.Session) (*Connection, PlanSnapshot, DebugSnapshot) {
	t.Helper()
	plan := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
		return runtimeDebugger, nil
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{
		Source:     api.Source{Content: "RETURN 1"},
		Debuggable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}

	return connection, compiled, opened
}

func TestSlowExecutionWatcherIsDetachedWithoutBlockingCompletion(t *testing.T) {
	limits := testLimits()
	limits.MaxWatchersPerResource = 1
	release := make(chan struct{})
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return &spySession{run: func(context.Context) (api.Output, error) {
			<-release

			return api.Output{}, nil
		}}, nil
	}}
	host, err := NewHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}
	started, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := connection.execution(started.ID)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := execution.subscribe()
	if err != nil {
		t.Fatal(err)
	}

	execution.mu.Lock()
	for range watcherBufferSize + 1 {
		execution.publishLocked(ExecutionEventStarted, false)
	}
	execution.mu.Unlock()
	select {
	case err := <-subscription.Errors:
		if !errors.Is(err, ErrWatcherLagged) {
			t.Fatalf("unexpected watcher error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow watcher was not detached")
	}
	if _, err := execution.subscribe(); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("detached handler released watcher slot early: %v", err)
	}
	subscription.Cancel()
	if next, err := execution.subscribe(); err != nil {
		t.Fatalf("watcher slot was not released: %v", err)
	} else {
		next.Cancel()
	}
	close(release)
	waitExecution(t, connection, started.ID)
}

func TestSlowDebugWatcherIsDetachedAndRetainsSlotUntilCancelled(t *testing.T) {
	limits := testLimits()
	limits.MaxWatchersPerResource = 1
	plan := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
		return &spyDebugger{}, nil
	}}
	host, err := NewHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := connection.debugSession(opened.ID)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := session.subscribe()
	if err != nil {
		t.Fatal(err)
	}

	session.mu.Lock()
	for range watcherBufferSize + 1 {
		session.publishLocked(DebugEventStarted, false)
	}
	session.mu.Unlock()
	select {
	case err := <-subscription.Errors:
		if !errors.Is(err, ErrWatcherLagged) {
			t.Fatalf("unexpected watcher error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow debug watcher was not detached")
	}
	if _, err := session.subscribe(); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("detached debug handler released watcher slot early: %v", err)
	}
	subscription.Cancel()
	if next, err := session.subscribe(); err != nil {
		t.Fatalf("debug watcher slot was not released: %v", err)
	} else {
		next.Cancel()
	}
}

func waitDebugState(t *testing.T, connection *Connection, id DebugSessionID, state DebugState) DebugSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		session, err := connection.debugSession(id)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := session.snapshot()
		if snapshot.State == state {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("debug session did not reach state %d", state)

	return DebugSnapshot{}
}
