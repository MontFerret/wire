package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MontFerret/ferret/v2"
)

func TestSlowDebugWatcherIsDetachedWithoutBlockingCommands(t *testing.T) {
	engine, err := ferret.New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	runtime, err := NewRuntime(engine, RuntimeInfo{})
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

func TestConcurrentReleaseWaitsForTheSameResult(t *testing.T) {
	engine, err := ferret.New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	runtime, err := NewRuntime(engine, RuntimeInfo{})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtime.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := connection.Compile(context.Background(), CompileInput{Content: "RETURN 1"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: plan.ID})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := connection.WatchExecution(execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Cancel()
	for {
		if subscription.Current.Kind == ExecutionEventCompleted {
			break
		}
		event, ok := <-subscription.Events
		if !ok {
			break
		}
		if event.Kind == ExecutionEventCompleted {
			break
		}
	}

	const callers = 16
	start := make(chan struct{})
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			errs <- connection.ReleaseExecution(context.Background(), execution.ID)
		}()
	}
	close(start)
	for range callers {
		if err := <-errs; err != nil {
			t.Fatalf("release failed: %v", err)
		}
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
	runtime, err := NewRuntime(engine, RuntimeInfo{})
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
	plans := len(connection.plans) + len(connection.releasedPlans)
	connection.mu.RUnlock()
	if plans != 0 {
		t.Fatalf("connection retained %d plans after close", plans)
	}
}
