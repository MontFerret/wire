package core

import (
	"context"
	"errors"
	wireruntime "github.com/MontFerret/wire/pkg/runtime"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

func TestConnectionCloseIsIdempotentAndRejectsNewResources(t *testing.T) {
	closeStarted := make(chan struct{})
	finishClose := make(chan struct{})
	plan := &spyPlan{close: func() error {
		close(closeStarted)
		<-finishClose

		return nil
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

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- connection.Close(context.Background()) }()
	<-closeStarted

	if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 2"}}); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("compile was accepted while connection was closing: %v", err)
	}

	if _, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID}); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("execution was accepted while connection was closing: %v", err)
	}

	if _, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID}); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("debug session was accepted while connection was closing: %v", err)
	}

	go func() { second <- connection.Close(context.Background()) }()
	select {
	case err := <-second:
		t.Fatalf("concurrent close returned before cleanup settled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(finishClose)
	for index, result := range []<-chan error{first, second} {
		if err := <-result; err != nil {
			t.Fatalf("connection close %d failed: %v", index, err)
		}
	}

	if err := connection.Close(context.Background()); err != nil {
		t.Fatalf("settled connection close was not idempotent: %v", err)
	}

	_, _, closeCalls := plan.snapshot()
	if closeCalls != 1 {
		t.Fatalf("API plan closed %d times", closeCalls)
	}
}

func TestPlanReleaseWaitsForInFlightDebugCreation(t *testing.T) {
	constructorStarted := make(chan struct{})
	finishConstructor := make(chan struct{})
	planCloseStarted := make(chan struct{})
	var orderMu sync.Mutex
	var order []string
	record := func(value string) {
		orderMu.Lock()
		order = append(order, value)
		orderMu.Unlock()
	}
	runtimeDebugger := &spyDebugger{close: func() error {
		record("debug")

		return nil
	}}
	plan := &spyPlan{
		newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
			close(constructorStarted)
			<-finishConstructor

			return runtimeDebugger, nil
		},
		close: func() error {
			record("plan")
			close(planCloseStarted)

			return nil
		},
	}
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
	retained, err := connection.plans.lookup(compiled.ID)
	if err != nil {
		t.Fatal(err)
	}

	openResult := make(chan error, 1)
	go func() {
		_, openErr := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
		openResult <- openErr
	}()
	<-constructorStarted

	releaseResult := make(chan error, 1)
	go func() { releaseResult <- connection.ReleasePlan(context.Background(), compiled.ID) }()
	waitPlanClosing(t, retained)

	if _, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID}); !hasCategory(err, ErrorKindPlanNotFound) {
		t.Fatalf("closing plan accepted an execution: %v", err)
	}
	if _, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID}); !hasCategory(err, ErrorKindPlanNotFound) {
		t.Fatalf("closing plan accepted another debug session: %v", err)
	}

	select {
	case <-planCloseStarted:
		t.Fatal("API plan closed while debug construction was in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(finishConstructor)
	if err := <-openResult; !hasCategory(err, ErrorKindPlanNotFound) {
		t.Fatalf("in-flight debug construction committed after plan release: %v", err)
	}

	if err := <-releaseResult; err != nil {
		t.Fatal(err)
	}

	orderMu.Lock()
	settledOrder := append([]string(nil), order...)
	orderMu.Unlock()
	if !reflect.DeepEqual(settledOrder, []string{"debug", "plan"}) {
		t.Fatalf("unexpected cleanup order: %#v", settledOrder)
	}
}

func TestPlanReleaseWaitsForChildrenAlreadyClosing(t *testing.T) {
	t.Run("execution", func(t *testing.T) {
		childCloseStarted := make(chan struct{})
		finishChildClose := make(chan struct{})
		planCloseStarted := make(chan struct{})
		runStarted := make(chan struct{})
		var orderMu sync.Mutex
		var order []string
		record := func(value string) {
			orderMu.Lock()
			order = append(order, value)
			orderMu.Unlock()
		}
		session := &spySession{
			run: func(ctx context.Context) (api.Output, error) {
				close(runStarted)
				<-ctx.Done()

				return api.Output{}, ctx.Err()
			},
			close: func() error {
				close(childCloseStarted)
				<-finishChildClose
				record("execution")

				return nil
			},
		}
		plan := &spyPlan{
			newSession: func(context.Context, sessionOptions) (api.Session, error) {
				return session, nil
			},
			close: func() error {
				record("plan")
				close(planCloseStarted)

				return nil
			},
		}
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return plan, nil
		}})
		compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
		if err != nil {
			t.Fatal(err)
		}
		retained, err := connection.plans.lookup(compiled.ID)
		if err != nil {
			t.Fatal(err)
		}
		execution, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
		if err != nil {
			t.Fatal(err)
		}
		<-runStarted

		childResult := make(chan error, 1)
		go func() { childResult <- connection.ReleaseExecution(context.Background(), execution.ID) }()
		<-childCloseStarted
		planResult := make(chan error, 1)
		go func() { planResult <- connection.ReleasePlan(context.Background(), compiled.ID) }()
		waitPlanClosing(t, retained)

		select {
		case <-planCloseStarted:
			t.Fatal("API plan closed before closing execution settled")
		case <-time.After(50 * time.Millisecond):
		}

		close(finishChildClose)
		if err := <-childResult; err != nil {
			t.Fatal(err)
		}
		if err := <-planResult; err != nil {
			t.Fatal(err)
		}

		orderMu.Lock()
		settledOrder := append([]string(nil), order...)
		orderMu.Unlock()
		if !reflect.DeepEqual(settledOrder, []string{"execution", "plan"}) {
			t.Fatalf("unexpected cleanup order: %#v", settledOrder)
		}
	})

	t.Run("debug session", func(t *testing.T) {
		childCloseStarted := make(chan struct{})
		finishChildClose := make(chan struct{})
		planCloseStarted := make(chan struct{})
		var orderMu sync.Mutex
		var order []string
		record := func(value string) {
			orderMu.Lock()
			order = append(order, value)
			orderMu.Unlock()
		}
		runtimeDebugger := &spyDebugger{close: func() error {
			close(childCloseStarted)
			<-finishChildClose
			record("debug")

			return nil
		}}
		plan := &spyPlan{
			newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
				return runtimeDebugger, nil
			},
			close: func() error {
				record("plan")
				close(planCloseStarted)

				return nil
			},
		}
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
		retained, err := connection.plans.lookup(compiled.ID)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
		if err != nil {
			t.Fatal(err)
		}

		childResult := make(chan error, 1)
		go func() { childResult <- connection.ReleaseDebugSession(context.Background(), opened.ID) }()
		<-childCloseStarted
		planResult := make(chan error, 1)
		go func() { planResult <- connection.ReleasePlan(context.Background(), compiled.ID) }()
		waitPlanClosing(t, retained)

		select {
		case <-planCloseStarted:
			t.Fatal("API plan closed before closing debug session settled")
		case <-time.After(50 * time.Millisecond):
		}

		close(finishChildClose)
		if err := <-childResult; err != nil {
			t.Fatal(err)
		}
		if err := <-planResult; err != nil {
			t.Fatal(err)
		}

		orderMu.Lock()
		settledOrder := append([]string(nil), order...)
		orderMu.Unlock()
		if !reflect.DeepEqual(settledOrder, []string{"debug", "plan"}) {
			t.Fatalf("unexpected cleanup order: %#v", settledOrder)
		}
	})
}

func TestConcurrentChildReleaseSharesCleanup(t *testing.T) {
	t.Run("execution", func(t *testing.T) {
		closeStarted := make(chan struct{})
		finishClose := make(chan struct{})
		runStarted := make(chan struct{})
		session := &spySession{
			run: func(ctx context.Context) (api.Output, error) {
				close(runStarted)
				<-ctx.Done()

				return api.Output{}, ctx.Err()
			},
			close: func() error {
				close(closeStarted)
				<-finishClose

				return nil
			},
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
		<-runStarted

		first := make(chan error, 1)
		second := make(chan error, 1)
		go func() { first <- connection.ReleaseExecution(context.Background(), execution.ID) }()
		<-closeStarted
		go func() { second <- connection.ReleaseExecution(context.Background(), execution.ID) }()
		select {
		case err := <-second:
			t.Fatalf("concurrent execution release returned early: %v", err)
		case <-time.After(50 * time.Millisecond):
		}

		close(finishClose)
		for index, result := range []<-chan error{first, second} {
			if err := <-result; err != nil {
				t.Fatalf("execution release %d failed: %v", index, err)
			}
		}

		_, closeCalls := session.counts()
		if closeCalls != 1 {
			t.Fatalf("execution session closed %d times", closeCalls)
		}
	})

	t.Run("debug session", func(t *testing.T) {
		closeStarted := make(chan struct{})
		finishClose := make(chan struct{})
		closeErr := errors.New("debug close failed")
		runtimeDebugger := &spyDebugger{close: func() error {
			close(closeStarted)
			<-finishClose

			return closeErr
		}}
		connection, _, opened := openTestDebugSession(t, runtimeDebugger)

		first := make(chan error, 1)
		second := make(chan error, 1)
		go func() { first <- connection.ReleaseDebugSession(context.Background(), opened.ID) }()
		<-closeStarted
		go func() { second <- connection.ReleaseDebugSession(context.Background(), opened.ID) }()
		select {
		case err := <-second:
			t.Fatalf("concurrent debug release returned early: %v", err)
		case <-time.After(50 * time.Millisecond):
		}

		close(finishClose)
		for index, result := range []<-chan error{first, second} {
			if err := <-result; !errors.Is(err, closeErr) {
				t.Fatalf("debug release %d did not retain its result: %v", index, err)
			}
		}

		if closeCalls := runtimeDebugger.closes(); closeCalls != 1 {
			t.Fatalf("runtime debug session closed %d times", closeCalls)
		}
	})
}

func TestExecutionTerminalStateSurvivesCancellationOrdering(t *testing.T) {
	t.Run("cancellation wins before completion", func(t *testing.T) {
		runStarted := make(chan struct{})
		finishRun := make(chan struct{})
		plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
			return &spySession{run: func(context.Context) (api.Output, error) {
				close(runStarted)
				<-finishRun

				return api.Output{}, nil
			}}, nil
		}}
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return plan, nil
		}})
		compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
		if err != nil {
			t.Fatal(err)
		}
		started, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
		if err != nil {
			t.Fatal(err)
		}
		<-runStarted

		if _, err := connection.CancelExecution(started.ID); err != nil {
			t.Fatal(err)
		}
		close(finishRun)
		settled := waitExecution(t, connection, started.ID)
		if settled.State != wireruntime.StateCancelled {
			t.Fatalf("cancellation did not retain the terminal state: %#v", settled)
		}
	})

	t.Run("completion remains terminal after cancellation", func(t *testing.T) {
		plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
			return &spySession{run: func(context.Context) (api.Output, error) {
				return api.Output{}, nil
			}}, nil
		}}
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return plan, nil
		}})
		compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}})
		if err != nil {
			t.Fatal(err)
		}
		started, err := connection.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
		if err != nil {
			t.Fatal(err)
		}
		settled := waitExecution(t, connection, started.ID)
		if settled.State != wireruntime.StateCompleted {
			t.Fatalf("execution did not complete: %#v", settled)
		}

		afterCancel, err := connection.CancelExecution(started.ID)
		if err != nil {
			t.Fatal(err)
		}
		if afterCancel.State != wireruntime.StateCompleted {
			t.Fatalf("late cancellation changed terminal state: %#v", afterCancel)
		}
	})
}

func waitPlanClosing(t *testing.T, plan *Plan) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		plan.mu.Lock()
		closing := plan.closing
		plan.mu.Unlock()
		if closing {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("plan did not begin closing")
}
