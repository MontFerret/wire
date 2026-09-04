package server_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type (
	contractRuntime struct {
		mu             sync.Mutex
		run            func(context.Context, api.Source, apiSessionOptions) (api.Output, error)
		compile        func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error)
		runSources     []api.Source
		runOptions     []apiSessionOptions
		compileSources []api.Source
		compileDebug   []bool
		compileLevels  []*api.OptimizationLevel
		closeCalls     int
	}

	contractPlan struct {
		mu              sync.Mutex
		params          []string
		newSession      func(context.Context, apiSessionOptions) (api.Session, error)
		newDebugSession func(context.Context, apiSessionOptions) (debugger.Session, error)
		sessionOptions  []apiSessionOptions
		debugOptions    []apiSessionOptions
		closeCalls      int
	}

	contractSession struct {
		mu         sync.Mutex
		run        func(context.Context, int) (api.Output, error)
		runCalls   int
		closeCalls int
	}

	contractPlanOptions struct {
		optimizationLevel *api.OptimizationLevel
	}

	contractDebugger struct {
		mu               sync.Mutex
		continueStarted  chan struct{}
		pauseRequested   chan struct{}
		breakpoints      map[debugger.BreakpointID]debugger.Breakpoint
		nextBreakpointID debugger.BreakpointID
		commands         []string
		frameLocals      []int
		evaluateFrames   []int
		pauseOnce        sync.Once
		continueCalls    int
		closeCalls       int
	}
)

func TestRuntimeAdapterDirectRunPreservesContractAndBorrowedOwnership(t *testing.T) {
	started := make(chan struct{})
	settled := make(chan struct{})
	var cancelOnce sync.Once
	hosted := &contractRuntime{}
	hosted.run = func(ctx context.Context, src api.Source, options apiSessionOptions) (api.Output, error) {
		switch src.Content {
		case "partial":
			return api.Output{ContentType: "application/json", Content: []byte(`{"partial":true}`)}, errors.New("host failure")
		case "cancel":
			cancelOnce.Do(func() { close(started) })
			<-ctx.Done()
			close(settled)

			return api.Output{ContentType: "text/plain", Content: []byte("partial")}, ctx.Err()
		default:
			return api.Output{ContentType: options.contentType, Content: []byte(`{"ok":true}`)}, nil
		}
	}
	hosted.compile = func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error) {
		return &contractPlan{}, nil
	}
	env := newIntegrationEnv(t, hosted)
	remote, err := client.NewRuntime(testContext(t), env.conn)
	if err != nil {
		t.Fatal(err)
	}

	output, err := remote.Run(
		testContext(t),
		api.Source{Name: "direct.fql", Content: "success"},
		api.WithParam("input", map[string]any{"value": int64(7)}),
		api.WithOutputContentType("application/json"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if output.ContentType != "application/json" || string(output.Content) != `{"ok":true}` {
		t.Fatalf("unexpected direct runtime output: %#v", output)
	}

	partial, err := remote.Run(testContext(t), api.Source{Name: "partial.fql", Content: "partial"})
	var asynchronous *failure.Failure
	if !errors.As(err, &asynchronous) || asynchronous.Category != failure.CategoryExecution {
		t.Fatalf("unexpected asynchronous failure: %v", err)
	}
	if partial.ContentType != "application/json" || string(partial.Content) != `{"partial":true}` {
		t.Fatalf("partial output was lost: %#v", partial)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() {
		_, runErr := remote.Run(cancelCtx, api.Source{Name: "cancel.fql", Content: "cancel"})
		cancelled <- runErr
	}()
	<-started
	cancel()
	cancelErr := <-cancelled
	if !errors.Is(cancelErr, context.Canceled) && status.Code(cancelErr) != codes.Canceled {
		t.Fatalf("caller cancellation was not preserved: %v", cancelErr)
	}
	select {
	case <-settled:
	case <-time.After(5 * time.Second):
		t.Fatalf("cancelled Runtime.Run returned before remote cleanup settled: %T %v", cancelErr, cancelErr)
	}

	hosted.mu.Lock()
	sources := append([]api.Source(nil), hosted.runSources...)
	options := make([]apiSessionOptions, len(hosted.runOptions))
	for index, value := range hosted.runOptions {
		options[index] = value.clone()
	}
	hosted.mu.Unlock()
	if len(sources) != 3 || sources[0] != (api.Source{Name: "direct.fql", Content: "success"}) {
		t.Fatalf("hosted Runtime.Run did not receive exact source: %#v", sources)
	}
	if len(options) != 3 || options[0].contentType != "application/json" ||
		!reflect.DeepEqual(options[0].params, map[string]any{"input": map[string]any{"value": int64(7)}}) {
		t.Fatalf("hosted Runtime.Run did not receive exact options: %#v", options)
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
	if err := remote.Close(); err != nil {
		t.Fatalf("Runtime.Close did not retain its result: %v", err)
	}
	if _, err := env.client.Compile(testContext(t), api.Source{Content: "RETURN 1"}, client.CompileOptions{}); err != nil {
		t.Fatalf("Runtime.Close closed the caller-owned transport: %v", err)
	}
	hosted.mu.Lock()
	closeCalls := hosted.closeCalls
	hosted.mu.Unlock()
	if closeCalls != 0 {
		t.Fatalf("Wire closed the borrowed hosted Runtime %d times", closeCalls)
	}
}

func TestRuntimeAdapterCompilesReusableDurableSessions(t *testing.T) {
	var sessionsMu sync.Mutex
	var sessions []*contractSession
	hostedPlan := &contractPlan{
		params: []string{"input"},
		newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
			session := &contractSession{run: func(_ context.Context, call int) (api.Output, error) {
				return api.Output{ContentType: "text/plain", Content: []byte{byte('0' + call)}}, nil
			}}
			sessionsMu.Lock()
			sessions = append(sessions, session)
			sessionsMu.Unlock()

			return session, nil
		},
	}
	hosted := &contractRuntime{compile: func(_ context.Context, _ api.Source, debug bool, level *api.OptimizationLevel) (api.Plan, error) {
		if debug && level != nil && *level == api.OptimizationBasic {
			return hostedPlan, nil
		}

		return &contractPlan{params: []string{"input"}}, nil
	}}
	env := newIntegrationEnv(t, hosted)
	remote, err := client.NewRuntime(testContext(t), env.conn)
	if err != nil {
		t.Fatal(err)
	}

	var plan api.Plan
	for _, level := range []api.OptimizationLevel{
		api.OptimizationNone,
		api.OptimizationBasic,
		api.OptimizationFull,
		api.OptimizationAggressive,
	} {
		compiledPlan, err := remote.Compile(
			testContext(t),
			api.Source{Name: "compiled.fql", Content: "RETURN @input"},
			api.WithOptimizationLevel(level),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := compiledPlan.Close(); err != nil {
			t.Fatal(err)
		}
		debugPlan, err := remote.CompileDebug(
			testContext(t),
			api.Source{Name: "debug.fql", Content: "RETURN @input"},
			api.WithOptimizationLevel(level),
		)
		if err != nil {
			t.Fatal(err)
		}
		if level == api.OptimizationBasic {
			plan = debugPlan
		} else if err := debugPlan.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if plan == nil {
		t.Fatal("reusable debug Plan was not retained")
	}
	params := plan.Params()
	params[0] = "mutated"
	if !reflect.DeepEqual(plan.Params(), []string{"input"}) {
		t.Fatalf("Plan.Params was not defensive: %#v", plan.Params())
	}

	session, err := plan.NewSession(
		testContext(t),
		api.WithParam("input", int64(42)),
		api.WithOutputContentType("text/plain"),
	)
	if err != nil {
		t.Fatal(err)
	}
	for index, expected := range []string{"1", "2"} {
		output, err := session.Run(testContext(t))
		if err != nil {
			t.Fatalf("sequential run %d failed: %v", index, err)
		}
		if output.ContentType != "text/plain" || string(output.Content) != expected {
			t.Fatalf("unexpected sequential output %d: %#v", index, output)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Session.Close did not retain its result: %v", err)
	}
	concurrent := make([]api.Session, 2)
	for index := range concurrent {
		concurrent[index], err = plan.NewSession(testContext(t), api.WithParam("input", int64(index)))
		if err != nil {
			t.Fatal(err)
		}
	}
	type sessionRunResult struct {
		output api.Output
		err    error
	}
	runResults := make(chan sessionRunResult, len(concurrent))
	for _, value := range concurrent {
		go func() {
			output, runErr := value.Run(testContext(t))
			runResults <- sessionRunResult{output: output, err: runErr}
		}()
	}
	for range concurrent {
		result := <-runResults
		if result.err != nil || string(result.output.Content) != "1" {
			t.Fatalf("concurrent Session run failed: %#v, %v", result.output, result.err)
		}
	}
	for _, value := range concurrent {
		if err := value.Close(); err != nil {
			t.Fatal(err)
		}
	}

	sessionsMu.Lock()
	createdSessions := append([]*contractSession(nil), sessions...)
	sessionsMu.Unlock()
	if len(createdSessions) != 3 {
		t.Fatalf("three durable remote Sessions created %d hosted Sessions", len(createdSessions))
	}
	if runs, closes := createdSessions[0].counts(); runs != 2 || closes != 1 {
		t.Fatalf("unexpected hosted Session lifecycle: runs=%d closes=%d", runs, closes)
	}
	for index, value := range createdSessions[1:] {
		if runs, closes := value.counts(); runs != 1 || closes != 1 {
			t.Fatalf("unexpected concurrent hosted Session %d lifecycle: runs=%d closes=%d", index, runs, closes)
		}
	}

	hosted.mu.Lock()
	levels := cloneOptimizationLevels(hosted.compileLevels)
	debug := append([]bool(nil), hosted.compileDebug...)
	hosted.mu.Unlock()
	wantLevels := []api.OptimizationLevel{
		api.OptimizationNone,
		api.OptimizationNone,
		api.OptimizationBasic,
		api.OptimizationBasic,
		api.OptimizationFull,
		api.OptimizationFull,
		api.OptimizationAggressive,
		api.OptimizationAggressive,
	}
	if !reflect.DeepEqual(levels, wantLevels) ||
		!reflect.DeepEqual(debug, []bool{false, true, false, true, false, true, false, true}) {
		t.Fatalf("compile options were not preserved: levels=%#v debug=%#v", levels, debug)
	}
	hostedPlan.mu.Lock()
	sessionOptions := append([]apiSessionOptions(nil), hostedPlan.sessionOptions...)
	hostedPlan.mu.Unlock()
	if len(sessionOptions) != 3 || sessionOptions[0].contentType != "text/plain" ||
		!reflect.DeepEqual(sessionOptions[0].params, map[string]any{"input": int64(42)}) {
		t.Fatalf("session options were not preserved: %#v", sessionOptions)
	}

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeAdapterCompilePreservesDiagnostics(t *testing.T) {
	values := testDiagnostics()
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error) {
		return nil, errors.Join(errors.New("runtime compiler secret"), values)
	}}
	env := newIntegrationEnv(t, hosted)
	remote, err := client.NewRuntime(testContext(t), env.conn)
	if err != nil {
		t.Fatal(err)
	}

	for _, compile := range []func(context.Context, api.Source, ...api.PlanOption) (api.Plan, error){
		remote.Compile,
		remote.CompileDebug,
	} {
		_, err := compile(testContext(t), api.Source{Name: "query.fql", Content: "RETURN"})
		var wireErr *client.Error
		if !errors.As(err, &wireErr) || wireErr.Category != failure.CategoryCompilation ||
			!reflect.DeepEqual(wireErr.Diagnostics, values) {
			t.Fatalf("portable compile diagnostics changed: %#v", wireErr)
		}
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeAdapterSessionRejectsOverlapAndReopensAfterRelease(t *testing.T) {
	started := make(chan struct{})
	hostedSession := &contractSession{run: func(ctx context.Context, call int) (api.Output, error) {
		if call == 1 {
			close(started)
			<-ctx.Done()

			return api.Output{}, ctx.Err()
		}

		return api.Output{ContentType: "text/plain", Content: []byte("reused")}, nil
	}}
	hostedPlan := &contractPlan{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
		return hostedSession, nil
	}}
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error) {
		return hostedPlan, nil
	}}
	env := newIntegrationEnv(t, hosted)
	remote, err := client.NewRuntime(testContext(t), env.conn)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := remote.Compile(testContext(t), api.Source{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := plan.NewSession(testContext(t))
	if err != nil {
		t.Fatal(err)
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, runErr := session.Run(firstCtx)
		first <- runErr
	}()
	<-started
	_, err = session.Run(testContext(t))
	var wireErr *client.Error
	if !errors.As(err, &wireErr) || wireErr.Category != failure.CategoryInvalidState {
		t.Fatalf("overlapping Session.Run was not rejected: %v", err)
	}
	cancelFirst()
	if err := <-first; !cancellationError(err) {
		t.Fatalf("active Session.Run cancellation was not preserved: %v", err)
	}

	output, err := session.Run(testContext(t))
	if err != nil || string(output.Content) != "reused" {
		t.Fatalf("Session was not reusable after hidden Execution release: %#v, %v", output, err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeAdapterDebuggerBridge(t *testing.T) {
	hostedDebugger := newContractDebugger()
	hostedPlan := &contractPlan{newDebugSession: func(context.Context, apiSessionOptions) (debugger.Session, error) {
		return hostedDebugger, nil
	}}
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error) {
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

func TestNewRuntimePreservesUnavailableStatus(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient(
		"passthrough:///unavailable-wire-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	_, err = client.NewRuntime(testContext(t), connection)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("NewRuntime changed unavailable transport status: %v", err)
	}
}

func TestRuntimeAdapterReleasesAllocationsThatRaceCancellation(t *testing.T) {
	t.Run("plan", func(t *testing.T) {
		started := make(chan struct{})
		finish := make(chan struct{})
		hostedPlan := &contractPlan{}
		hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error) {
			close(started)
			<-finish

			return hostedPlan, nil
		}}
		env := newIntegrationEnv(t, hosted)
		remote, err := client.NewRuntime(testContext(t), env.conn)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, compileErr := remote.Compile(ctx, api.Source{Content: "RETURN 1"})
			result <- compileErr
		}()
		<-started
		cancel()
		close(finish)
		if err := <-result; !cancellationError(err) {
			t.Fatalf("compile cancellation was not preserved: %v", err)
		}
		hostedPlan.mu.Lock()
		closeCalls := hostedPlan.closeCalls
		hostedPlan.mu.Unlock()
		if closeCalls != 1 {
			t.Fatalf("Plan published during cancellation closed %d times", closeCalls)
		}
		if err := remote.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("normal session", func(t *testing.T) {
		started := make(chan struct{})
		finish := make(chan struct{})
		hostedSession := &contractSession{}
		hostedPlan := &contractPlan{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
			close(started)
			<-finish

			return hostedSession, nil
		}}
		hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error) {
			return hostedPlan, nil
		}}
		env := newIntegrationEnv(t, hosted)
		remote, err := client.NewRuntime(testContext(t), env.conn)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := remote.Compile(testContext(t), api.Source{Content: "RETURN 1"})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, createErr := plan.NewSession(ctx)
			result <- createErr
		}()
		<-started
		cancel()
		close(finish)
		if err := <-result; !cancellationError(err) {
			t.Fatalf("session cancellation was not preserved: %v", err)
		}
		if runs, closes := hostedSession.counts(); runs != 0 || closes != 1 {
			t.Fatalf("Session published during cancellation leaked: runs=%d closes=%d", runs, closes)
		}
		if err := plan.Close(); err != nil {
			t.Fatal(err)
		}
		if err := remote.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTransportLossCleansRuntimeAdapterDescendants(t *testing.T) {
	started := make(chan struct{})
	settled := make(chan struct{})
	var runOnce sync.Once
	hostedSession := &contractSession{run: func(ctx context.Context, _ int) (api.Output, error) {
		runOnce.Do(func() { close(started) })
		<-ctx.Done()
		close(settled)

		return api.Output{}, ctx.Err()
	}}
	hostedPlan := &contractPlan{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
		return hostedSession, nil
	}}
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, *api.OptimizationLevel) (api.Plan, error) {
		return hostedPlan, nil
	}}
	env := newIntegrationEnv(t, hosted)
	remote, err := client.NewRuntime(testContext(t), env.conn)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := remote.Compile(testContext(t), api.Source{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := plan.NewSession(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	runResult := make(chan error, 1)
	go func() {
		_, runErr := session.Run(context.Background())
		runResult <- runErr
	}()
	<-started

	if err := env.conn.Close(); err != nil {
		t.Fatal(err)
	}
	env.shutdown = true
	env.transportClosed = true
	if err := <-runResult; status.Code(err) != codes.Unavailable && status.Code(err) != codes.Canceled {
		t.Fatalf("transport loss status was not preserved: %v", err)
	}
	select {
	case <-settled:
	case <-time.After(5 * time.Second):
		t.Fatal("transport loss did not cancel hosted Session.Run")
	}

	deadline := time.Now().Add(5 * time.Second)
	cleanupSettled := false
	for time.Now().Before(deadline) {
		_, sessionCloseCalls := hostedSession.counts()
		hostedPlan.mu.Lock()
		planCloseCalls := hostedPlan.closeCalls
		hostedPlan.mu.Unlock()
		if sessionCloseCalls == 1 && planCloseCalls == 1 {
			cleanupSettled = true

			break
		}
		time.Sleep(time.Millisecond)
	}
	if !cleanupSettled {
		_, sessionCloseCalls := hostedSession.counts()
		hostedPlan.mu.Lock()
		planCloseCalls := hostedPlan.closeCalls
		hostedPlan.mu.Unlock()
		t.Fatalf("transport loss cleanup did not settle: session=%d plan=%d", sessionCloseCalls, planCloseCalls)
	}
	hosted.mu.Lock()
	runtimeCloseCalls := hosted.closeCalls
	hosted.mu.Unlock()
	if runtimeCloseCalls != 0 {
		t.Fatalf("transport loss closed borrowed Runtime %d times", runtimeCloseCalls)
	}
}

func cancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

func (r *contractRuntime) Run(ctx context.Context, src api.Source, options ...api.SessionOption) (api.Output, error) {
	configured, err := applyAPIOptions(options)
	if err != nil {
		return api.Output{}, err
	}

	r.mu.Lock()
	r.runSources = append(r.runSources, src)
	r.runOptions = append(r.runOptions, configured.clone())
	run := r.run
	r.mu.Unlock()
	if run == nil {
		return api.Output{}, nil
	}

	return run(ctx, src, configured)
}

func (r *contractRuntime) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, false, options)
}

func (r *contractRuntime) CompileDebug(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, true, options)
}

func (r *contractRuntime) compilePlan(
	ctx context.Context,
	src api.Source,
	debug bool,
	options []api.PlanOption,
) (api.Plan, error) {
	configured := &contractPlanOptions{}
	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(configured); err != nil {
			return nil, err
		}
	}

	r.mu.Lock()
	r.compileSources = append(r.compileSources, src)
	r.compileDebug = append(r.compileDebug, debug)
	if configured.optimizationLevel == nil {
		r.compileLevels = append(r.compileLevels, nil)
	} else {
		level := *configured.optimizationLevel
		r.compileLevels = append(r.compileLevels, &level)
	}
	compile := r.compile
	r.mu.Unlock()
	if compile == nil {
		return &contractPlan{}, nil
	}

	return compile(ctx, src, debug, configured.optimizationLevel)
}

func (r *contractRuntime) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()

	return nil
}

func (o *contractPlanOptions) SetOptimizationLevel(level api.OptimizationLevel) error {
	o.optimizationLevel = &level

	return nil
}

func (p *contractPlan) Params() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.params
}

func (p *contractPlan) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	configured, err := applyAPIOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.sessionOptions = append(p.sessionOptions, configured.clone())
	create := p.newSession
	p.mu.Unlock()
	if create == nil {
		return &contractSession{}, nil
	}

	return create(ctx, configured)
}

func (p *contractPlan) NewDebugSession(ctx context.Context, options ...api.SessionOption) (debugger.Session, error) {
	configured, err := applyAPIOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.debugOptions = append(p.debugOptions, configured.clone())
	create := p.newDebugSession
	p.mu.Unlock()
	if create == nil {
		return nil, errors.New("debug session is not configured")
	}

	return create(ctx, configured)
}

func (p *contractPlan) Close() error {
	p.mu.Lock()
	p.closeCalls++
	p.mu.Unlock()

	return nil
}

func (s *contractSession) Run(ctx context.Context) (api.Output, error) {
	s.mu.Lock()
	s.runCalls++
	call := s.runCalls
	run := s.run
	s.mu.Unlock()
	if run == nil {
		return api.Output{}, nil
	}

	return run(ctx, call)
}

func (s *contractSession) Close() error {
	s.mu.Lock()
	s.closeCalls++
	s.mu.Unlock()

	return nil
}

func (s *contractSession) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.runCalls, s.closeCalls
}

func applyAPIOptions(options []api.SessionOption) (apiSessionOptions, error) {
	configured := apiSessionOptions{params: make(map[string]any)}
	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(&configured); err != nil {
			return apiSessionOptions{}, err
		}
	}

	return configured, nil
}

func cloneOptimizationLevels(values []*api.OptimizationLevel) []api.OptimizationLevel {
	result := make([]api.OptimizationLevel, len(values))
	for index, value := range values {
		if value != nil {
			result[index] = *value
		}
	}

	return result
}

func newContractDebugger() *contractDebugger {
	return &contractDebugger{
		continueStarted: make(chan struct{}),
		pauseRequested:  make(chan struct{}),
		breakpoints:     make(map[debugger.BreakpointID]debugger.Breakpoint),
	}
}

func (d *contractDebugger) Start(context.Context) (*debugger.Event, error) {
	d.recordCommand("start")

	return &debugger.Event{
		Reason: debugger.ReasonEntry,
		Location: source.Range{Location: source.Location{
			SourceName: "debug.fql",
			Position:   source.Position{Line: 1, Column: 1},
		}},
		Depth: 2,
	}, nil
}

func (d *contractDebugger) Continue(context.Context) (*debugger.Event, error) {
	d.recordCommand("continue")
	d.mu.Lock()
	d.continueCalls++
	call := d.continueCalls
	d.mu.Unlock()
	if call == 1 {
		close(d.continueStarted)
		<-d.pauseRequested

		return &debugger.Event{Reason: debugger.ReasonPause}, nil
	}

	return &debugger.Event{
		Reason: debugger.ReasonCompleted,
		Output: &api.Output{ContentType: "application/json", Content: []byte(`{"done":true}`)},
	}, nil
}

func (d *contractDebugger) StepIn(context.Context) (*debugger.Event, error) {
	d.recordCommand("step-in")

	return &debugger.Event{
		Reason:           debugger.ReasonBreakpoint,
		HitBreakpointIDs: []debugger.BreakpointID{2},
		Location: source.Range{
			Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 1, Column: 3}},
			Span:     source.Span{Start: 3, End: 4},
		},
		Depth: 3,
	}, nil
}

func (d *contractDebugger) StepOver(context.Context) (*debugger.Event, error) {
	d.recordCommand("step-over")

	return &debugger.Event{Reason: debugger.ReasonStep}, nil
}

func (d *contractDebugger) StepOut(context.Context) (*debugger.Event, error) {
	d.recordCommand("step-out")

	return &debugger.Event{Reason: debugger.ReasonRuntimeError, Error: errors.New("runtime error")}, nil
}

func (d *contractDebugger) Pause() error {
	d.pauseOnce.Do(func() { close(d.pauseRequested) })

	return nil
}

func (d *contractDebugger) SetBreakpoint(location source.Location) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(location, debugger.BreakpointOptions{})
}

func (d *contractDebugger) SetBreakpointAt(
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextBreakpointID++
	value := debugger.Breakpoint{
		ID:                d.nextBreakpointID,
		RequestedLocation: location,
		Location:          source.Range{Location: location, Span: source.Span{Start: 0, End: 1}},
		PointID:           debugger.PointID(10 + d.nextBreakpointID),
		FunctionID:        7,
		BindingMode:       options.BindingMode,
		Bound:             true,
	}
	d.breakpoints[value.ID] = value

	return value, nil
}

func (d *contractDebugger) DeleteBreakpoint(id debugger.BreakpointID) error {
	d.mu.Lock()
	delete(d.breakpoints, id)
	d.mu.Unlock()

	return nil
}

func (d *contractDebugger) Breakpoints() []debugger.Breakpoint {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]debugger.Breakpoint, 0, len(d.breakpoints))
	for _, value := range d.breakpoints {
		result = append(result, value)
	}

	return result
}

func (d *contractDebugger) Frames() ([]debugger.Frame, error) {
	return []debugger.Frame{
		{Name: "top", Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 2}}, FunctionID: 7},
		{Name: "caller", Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 1}}, FunctionID: 6},
	}, nil
}

func (d *contractDebugger) Locals() ([]debugger.Variable, error) {
	return d.FrameLocals(0)
}

func (d *contractDebugger) FrameLocals(frame int) ([]debugger.Variable, error) {
	d.mu.Lock()
	d.frameLocals = append(d.frameLocals, frame)
	d.mu.Unlock()

	return []debugger.Variable{{
		Name:    "local",
		Value:   debugger.Value{Type: "object", Display: "{...}", Reference: 9},
		Mutable: true,
	}}, nil
}

func (d *contractDebugger) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	if reference != 9 {
		return nil, errors.New("unexpected reference")
	}

	return []debugger.Variable{{Name: "child", Value: debugger.Value{Type: "string", Display: "value"}}}, nil
}

func (d *contractDebugger) Evaluate(ctx context.Context, expression string) (debugger.Value, error) {
	return d.EvaluateFrame(ctx, 0, expression)
}

func (d *contractDebugger) EvaluateFrame(
	_ context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
	d.mu.Lock()
	d.evaluateFrames = append(d.evaluateFrames, frame)
	d.mu.Unlock()

	return debugger.Value{Type: "string", Display: "frame-" + string(rune('0'+frame)) + ":" + expression}, nil
}

func (d *contractDebugger) Close() error {
	d.mu.Lock()
	d.closeCalls++
	d.mu.Unlock()

	return nil
}

func (d *contractDebugger) recordCommand(command string) {
	d.mu.Lock()
	d.commands = append(d.commands, command)
	d.mu.Unlock()
}
