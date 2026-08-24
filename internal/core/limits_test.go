package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/ferret/v2"
	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
)

func TestResourceLimitsCoverActiveResources(t *testing.T) {
	limits := testLimits()
	limits.MaxConnections = 1
	limits.MaxPlansPerConnection = 1
	limits.MaxExecutionsPerConnection = 1
	limits.MaxDebugSessionsPerConnection = 1
	limits.MaxWatchersPerResource = 1
	limits.MaxBreakpointsPerDebugSession = 1

	engine, err := ferret.New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runtime, err := NewRuntime(engine, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	if _, err := runtime.OpenConnection(); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected connection limit result: %v", err)
	}

	plan, err := connection.Compile(context.Background(), CompileInput{
		Content: "LET value = 1\nRETURN value", Identity: "limits.fql", Debuggable: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 2"}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected plan limit result: %v", err)
	}

	execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.Execute(context.Background(), ExecuteInput{PlanID: plan.ID}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected execution limit result: %v", err)
	}
	executionEvents, err := connection.WatchExecution(execution.ID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.WatchExecution(execution.ID); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected execution watcher limit result: %v", err)
	}
	executionEvents.Cancel()
	if replacement, err := connection.WatchExecution(execution.ID); err != nil {
		t.Fatalf("released execution watcher slot was not reusable: %v", err)
	} else {
		replacement.Cancel()
	}

	session, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: plan.ID}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected debug session limit result: %v", err)
	}
	debugEvents, err := connection.WatchDebug(session.ID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.WatchDebug(session.ID); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected debug watcher limit result: %v", err)
	}
	debugEvents.Cancel()
	if replacement, err := connection.WatchDebug(session.ID); err != nil {
		t.Fatalf("released debug watcher slot was not reusable: %v", err)
	} else {
		replacement.Cancel()
	}

	if _, err := connection.SetBreakpoint(context.Background(), session.ID, Location{File: "limits.fql", Line: 1}); err != nil {
		t.Fatal(err)
	}

	if _, err := connection.SetBreakpoint(context.Background(), session.ID, Location{File: "limits.fql", Line: 2}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("unexpected breakpoint limit result: %v", err)
	}
}

func TestPendingCreationCountsAgainstLimitsAndConnectionCloseWaits(t *testing.T) {
	compileEntered := make(chan struct{})
	allowCompile := make(chan struct{})
	engine, err := ferret.New(ferret.WithBeforeCompileHook(func(context.Context) error {
		close(compileEntered)
		<-allowCompile

		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	limits := testLimits()
	limits.MaxPlansPerConnection = 1
	runtime, err := NewRuntime(engine, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	compileResult := make(chan error, 1)
	go func() {
		_, compileErr := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1"})
		compileResult <- compileErr
	}()
	select {
	case <-compileEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("compile did not enter the host hook")
	}

	if _, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 2"}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("pending compile did not count against the plan limit: %v", err)
	}

	closeResult := make(chan error, 1)
	go func() { closeResult <- connection.Close(context.Background()) }()
	select {
	case err := <-closeResult:
		t.Fatalf("connection close returned before pending compile settled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowCompile)
	if err := <-compileResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("pending compile was not cancelled: %v", err)
	}

	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
}

func TestPendingDebugCreationCountsAgainstLimit(t *testing.T) {
	executionStarted := make(chan struct{})
	engine, err := ferret.New(
		ferret.WithMaxActiveSessions(1),
		ferret.WithFunctionsRegistrar(func(namespace ferretruntime.Namespace) {
			namespace.Function().A0().Add("PENDING_DEBUG_GATE", func(ctx context.Context) (ferretruntime.Value, error) {
				close(executionStarted)
				<-ctx.Done()

				return ferretruntime.None, ctx.Err()
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	limits := testLimits()
	limits.MaxDebugSessionsPerConnection = 1
	runtime, err := NewRuntime(engine, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := connection.Compile(context.Background(), CompileInput{
		Content: "RETURN PENDING_DEBUG_GATE()", Debuggable: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.Execute(context.Background(), ExecuteInput{PlanID: plan.ID}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executionStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("execution did not acquire the Ferret session slot")
	}

	openResult := make(chan error, 1)
	go func() {
		_, openErr := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: plan.ID})
		openResult <- openErr
	}()
	waitForPendingDebug(t, connection)
	if _, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: plan.ID}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("pending debug creation did not count against the limit: %v", err)
	}

	if err := connection.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := <-openResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("pending debug creation was not cancelled: %v", err)
	}
}

func TestClosingPlanCountsAgainstLimitAndThenBecomesStale(t *testing.T) {
	closeEntered := make(chan struct{})
	allowClose := make(chan struct{})
	var once sync.Once
	engine, err := ferret.New(ferret.WithPlanCloseHook(func() error {
		once.Do(func() { close(closeEntered) })
		<-allowClose

		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	limits := testLimits()
	limits.MaxPlansPerConnection = 1
	runtime, err := NewRuntime(engine, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	plan, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}

	releaseResult := make(chan error, 1)
	go func() { releaseResult <- connection.ReleasePlan(context.Background(), plan.ID) }()
	select {
	case <-closeEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("plan cleanup did not start")
	}

	if _, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 2"}); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("closing plan did not count against the limit: %v", err)
	}
	close(allowClose)
	if err := <-releaseResult; err != nil {
		t.Fatal(err)
	}

	if err := connection.ReleasePlan(context.Background(), plan.ID); !hasCategory(err, ErrorPlanNotFound) {
		t.Fatalf("released plan did not become stale: %v", err)
	}

	if _, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 2"}); err != nil {
		t.Fatalf("completed cleanup did not release the plan slot: %v", err)
	}
}

func TestCleanupPanicIsSanitizedAndDoesNotRetainPlan(t *testing.T) {
	engine, err := ferret.New(ferret.WithPlanCloseHook(func() error {
		panic("host cleanup secret")
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runtime, err := NewRuntime(engine, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	plan, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}

	if err := connection.ReleasePlan(context.Background(), plan.ID); !hasCategory(err, ErrorInternal) {
		t.Fatalf("cleanup panic was not sanitized: %v", err)
	}

	if err := connection.ReleasePlan(context.Background(), plan.ID); !hasCategory(err, ErrorPlanNotFound) {
		t.Fatalf("panicking cleanup retained the plan: %v", err)
	}
}

func TestExecutionClosePanicStillCompletesAndCanBeReleased(t *testing.T) {
	engine, err := ferret.New(ferret.WithSessionCloseHook(func() error {
		panic("host execution cleanup secret")
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runtime, err := NewRuntime(engine, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	plan, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}
	active, err := connection.execution(execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-active.done:
	case <-time.After(10 * time.Second):
		t.Fatal("execution close panic stranded terminal completion")
	}

	snapshot := active.snapshot()
	if snapshot.State != ExecutionFailed || snapshot.Failure == nil || snapshot.Failure.Category != ErrorInternal {
		t.Fatalf("execution close panic was not sanitized: %+v", snapshot)
	}

	if err := connection.ReleaseExecution(context.Background(), execution.ID); err != nil {
		t.Fatalf("execution with a panicking close hook could not be released: %v", err)
	}

	if err := connection.ReleaseExecution(context.Background(), execution.ID); !hasCategory(err, ErrorExecutionNotFound) {
		t.Fatalf("released execution did not become stale: %v", err)
	}
}

func TestDebugClosePanicStillTerminatesWatchersAndCanBeReleased(t *testing.T) {
	engine, err := ferret.New(ferret.WithSessionCloseHook(func() error {
		panic("host debug cleanup secret")
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runtime, err := NewRuntime(engine, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	plan, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1", Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	session, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := connection.WatchDebug(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()

	if err := connection.ReleaseDebugSession(context.Background(), session.ID); !hasCategory(err, ErrorInternal) {
		t.Fatalf("debug close panic was not sanitized: %v", err)
	}
	select {
	case event, ok := <-subscription.Events:
		if !ok || event.Kind != DebugEventTerminated {
			t.Fatalf("unexpected terminal debug event after close panic: %+v, open=%t", event, ok)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("debug close panic stranded an attached watcher")
	}

	if err := connection.ReleaseDebugSession(context.Background(), session.ID); !hasCategory(err, ErrorDebugSessionNotFound) {
		t.Fatalf("released debug session did not become stale: %v", err)
	}
}

func TestConcurrentConnectionCloseSharesInflightResultThenBecomesStale(t *testing.T) {
	closeEntered := make(chan struct{})
	allowClose := make(chan struct{})
	engine, err := ferret.New(ferret.WithPlanCloseHook(func() error {
		close(closeEntered)
		<-allowClose

		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runtime, err := NewRuntime(engine, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1"}); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	go func() { first <- runtime.CloseConnection(context.Background(), connection.ID()) }()
	select {
	case <-closeEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("logical connection cleanup did not start")
	}
	second := make(chan error, 1)
	go func() { second <- runtime.CloseConnection(context.Background(), connection.ID()) }()
	time.Sleep(50 * time.Millisecond)
	close(allowClose)
	if err := <-first; err != nil {
		t.Fatal(err)
	}

	if err := <-second; err != nil {
		t.Fatalf("concurrent close did not share the retained result: %v", err)
	}

	if err := runtime.CloseConnection(context.Background(), connection.ID()); !hasCategory(err, ErrorConnectionNotFound) {
		t.Fatalf("completed logical connection did not become stale: %v", err)
	}
	runtime.mu.RLock()
	closing := len(runtime.closing)
	runtime.mu.RUnlock()
	if closing != 0 {
		t.Fatalf("runtime retained %d completed connection closes", closing)
	}
}

func hasCategory(err error, category ErrorCategory) bool {
	var domain *DomainError

	return errors.As(err, &domain) && domain.Category == category
}

func waitForPendingDebug(t *testing.T, connection *Connection) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		connection.mu.RLock()
		pending := connection.pendingDebug
		connection.mu.RUnlock()
		if pending > 0 {
			return
		}

		select {
		case <-deadline.C:
			t.Fatal("debug session creation did not become pending")
		case <-ticker.C:
		}
	}
}
