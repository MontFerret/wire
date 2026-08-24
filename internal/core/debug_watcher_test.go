package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MontFerret/ferret/v2"
	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
)

func TestSlowDebugWatcherIsDetachedWithoutBlockingCommands(t *testing.T) {
	engine, err := ferret.New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	limits := testLimits()
	limits.MaxWatchersPerResource = 2
	runtime, err := NewRuntime(engine, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())

	var source strings.Builder
	source.WriteString("VAR total = 0\n")
	for range 12 {
		source.WriteString("total = total + 1\n")
	}
	source.WriteString("RETURN total")
	plan, err := connection.Compile(context.Background(), CompileInput{Content: source.String(), Identity: "slow.fql", Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	session, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := connection.WatchDebug(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Cancel()
	fast, err := connection.WatchDebug(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Cancel()

	if _, err := connection.StartDebug(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	waitCoreDebugStop(t, fast)
	for range 5 {
		if _, err := connection.NextDebug(context.Background(), session.ID); err != nil {
			t.Fatal(err)
		}
		waitCoreDebugStop(t, fast)
	}

	select {
	case lagErr := <-slow.Errors:
		if !errors.Is(lagErr, ErrWatcherLagged) {
			t.Fatalf("unexpected watcher error: %v", lagErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("slow watcher was not detached")
	}

	if _, err := connection.WatchDebug(session.ID); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("lagged stream released its watcher slot before the handler exited: %v", err)
	}
	slow.Cancel()
	replacement, err := connection.WatchDebug(session.ID)
	if err != nil {
		t.Fatalf("watcher slot was not released when the handler exited: %v", err)
	}
	replacement.Cancel()

	if _, err := connection.NextDebug(context.Background(), session.ID); err != nil {
		t.Fatalf("slow watcher blocked a later debugger command: %v", err)
	}
	waitCoreDebugStop(t, fast)
}

func waitCoreDebugStop(t *testing.T, subscription DebugSubscription) {
	t.Helper()
	for {
		select {
		case event, ok := <-subscription.Events:
			if !ok {
				t.Fatal("debug subscription closed before a stop")
			}

			if event.Kind == DebugEventStopped {
				return
			}
		case err := <-subscription.Errors:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("debug command did not stop")
		}
	}
}

func TestSlowExecutionWatcherDoesNotBlockCompletionAndRetainsSlotUntilHandlerExit(t *testing.T) {
	executionStarted := make(chan struct{})
	allowExecution := make(chan struct{})
	engine, err := ferret.New(ferret.WithFunctionsRegistrar(func(namespace ferretruntime.Namespace) {
		namespace.Function().A0().Add("SLOW_EXECUTION", func(context.Context) (ferretruntime.Value, error) {
			close(executionStarted)
			<-allowExecution

			return ferretruntime.NewInt(1), nil
		})
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	limits := testLimits()
	limits.MaxWatchersPerResource = 1
	runtime, err := NewRuntime(engine, RuntimeInfo{}, limits)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	plan, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN SLOW_EXECUTION()"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-executionStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("execution did not start")
	}

	slow, err := connection.WatchExecution(execution.ID)
	if err != nil {
		t.Fatal(err)
	}

	if slow.Current.Kind != ExecutionEventStarted {
		t.Fatalf("unexpected initial snapshot: %#v", slow.Current)
	}
	close(allowExecution)
	current, err := connection.execution(execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-current.done:
	case <-time.After(10 * time.Second):
		t.Fatal("slow watcher blocked execution completion")
	}

	if _, err := connection.WatchExecution(execution.ID); !hasCategory(err, ErrorResourceExhausted) {
		t.Fatalf("slow terminal watcher released its slot before handler exit: %v", err)
	}
	slow.Cancel()
	if replacement, err := connection.WatchExecution(execution.ID); err != nil {
		t.Fatalf("slow watcher slot was not reusable after handler exit: %v", err)
	} else {
		replacement.Cancel()
	}
}

func TestConcurrentReleaseWaitsForTheSameResult(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	allowReturn := make(chan struct{})
	engine, err := ferret.New(ferret.WithFunctionsRegistrar(func(namespace ferretruntime.Namespace) {
		namespace.Function().A0().Add("BLOCK_RELEASE", func(ctx context.Context) (ferretruntime.Value, error) {
			close(started)
			<-ctx.Done()
			close(cancelled)
			<-allowReturn

			return ferretruntime.None, ctx.Err()
		})
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
	plan, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN BLOCK_RELEASE()"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("execution did not start")
	}

	const callers = 16
	errs := make(chan error, callers)
	go func() {
		errs <- connection.ReleaseExecution(context.Background(), execution.ID)
	}()
	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("release did not cancel execution")
	}

	for range callers - 1 {
		go func() {
			errs <- connection.ReleaseExecution(context.Background(), execution.ID)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(allowReturn)
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("release failed: %v", err)
		}
	}

	if err := connection.ReleaseExecution(context.Background(), execution.ID); !hasCategory(err, ErrorExecutionNotFound) {
		t.Fatalf("completed release did not become stale: %v", err)
	}

	if err := connection.ReleasePlan(context.Background(), plan.ID); err != nil {
		t.Fatal(err)
	}
}

func TestCompileRacingConnectionCloseCannotPublishAPlan(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	engine, err := ferret.New(ferret.WithBeforeCompileHook(func(context.Context) error {
		close(entered)
		<-release
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
	compileResult := make(chan error, 1)
	go func() {
		_, compileErr := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1"})
		compileResult <- compileErr
	}()
	<-entered
	closeResult := make(chan error, 1)
	go func() { closeResult <- connection.Close(context.Background()) }()
	<-connection.Context().Done()
	close(release)
	if err := <-compileResult; err == nil {
		t.Fatal("compile published a plan after connection close committed")
	}

	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	connection.mu.RLock()
	plans := len(connection.plans) + len(connection.closingPlans)
	connection.mu.RUnlock()
	if plans != 0 {
		t.Fatalf("connection retained %d plans after close", plans)
	}
}

func testLimits() Limits {
	return Limits{
		MaxConnections:                64,
		MaxPlansPerConnection:         128,
		MaxExecutionsPerConnection:    128,
		MaxDebugSessionsPerConnection: 32,
		MaxWatchersPerResource:        8,
		MaxBreakpointsPerDebugSession: 256,
	}
}
