package core

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
)

func TestDurableSessionRunsSequentiallyOnOneHostedSession(t *testing.T) {
	runtimeSession := &spySession{run: func(context.Context) (api.Output, error) {
		return api.Output{ContentType: "application/json", Content: []byte(`{"ok":true}`)}, nil
	}}
	plan := &spyPlan{
		params: []string{"input"},
		newSession: func(context.Context, sessionOptions) (api.Session, error) {
			return runtimeSession, nil
		},
	}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), compileRequest{Source: api.Source{Content: "RETURN @input"}})
	if err != nil {
		t.Fatal(err)
	}

	created, err := connection.CreateSession(context.Background(), sessionRequest{
		PlanID:            compiled.ID,
		Parameters:        map[string]any{"input": int64(42)},
		OutputContentType: "application/json",
	})
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		run, err := connection.RunSession(context.Background(), created)
		if err != nil {
			t.Fatal(err)
		}

		if run.Snapshot.State != wireexecution.StateRunning {
			t.Fatalf("Session.Run did not return the initial running snapshot: %+v", run)
		}

		terminal := waitExecution(t, connection, run.ID)
		if terminal.State != wireexecution.StateCompleted || terminal.Output == nil ||
			string(terminal.Output.Content) != `{"ok":true}` {
			t.Fatalf("unexpected terminal execution: %#v", terminal)
		}
		if err := connection.ReleaseExecution(testContext(t), run.ID); err != nil {
			t.Fatal(err)
		}
	}

	sessionOptions, _, _ := plan.snapshot()
	if len(sessionOptions) != 1 || sessionOptions[0].contentType != "application/json" ||
		!reflect.DeepEqual(sessionOptions[0].params, map[string]any{"input": int64(42)}) {
		t.Fatalf("unexpected durable session options: %#v", sessionOptions)
	}

	if runCalls, closeCalls := runtimeSession.counts(); runCalls != 2 || closeCalls != 0 {
		t.Fatalf("unexpected hosted session state before release: run=%d close=%d", runCalls, closeCalls)
	}

	if err := connection.ReleaseSession(testContext(t), created); err != nil {
		t.Fatal(err)
	}

	if runCalls, closeCalls := runtimeSession.counts(); runCalls != 2 || closeCalls != 1 {
		t.Fatalf("unexpected hosted session state after release: run=%d close=%d", runCalls, closeCalls)
	}

	if err := connection.ReleaseSession(testContext(t), created); !hasCategory(err, ErrorKindSessionNotFound) {
		t.Fatalf("released session ID did not become stale: %v", err)
	}
}

func TestDurableSessionRejectsOverlappingRunsUntilExecutionRelease(t *testing.T) {
	started := make(chan struct{})
	runtimeSession := &spySession{run: func(ctx context.Context) (api.Output, error) {
		close(started)
		<-ctx.Done()

		return api.Output{}, ctx.Err()
	}}
	connection, _, created := openTestSession(t, runtimeSession)
	first, err := connection.RunSession(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	if _, err := connection.RunSession(context.Background(), created); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("overlapping session run was accepted: %v", err)
	}

	if err := connection.ReleaseExecution(testContext(t), first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := connection.RunSession(context.Background(), created)
	if err != nil {
		t.Fatalf("session was not reusable after execution release: %v", err)
	}

	if err := connection.ReleaseExecution(testContext(t), second.ID); err != nil {
		t.Fatal(err)
	}
}

func TestDurableSessionReleaseCancelsRunBeforeExactlyOnceClose(t *testing.T) {
	started := make(chan struct{})
	var orderMu sync.Mutex
	var order []string
	runtimeSession := &spySession{
		run: func(ctx context.Context) (api.Output, error) {
			close(started)
			<-ctx.Done()
			orderMu.Lock()
			order = append(order, "run")
			orderMu.Unlock()

			return api.Output{}, ctx.Err()
		},
		close: func() error {
			orderMu.Lock()
			order = append(order, "close")
			orderMu.Unlock()

			return nil
		},
	}
	connection, compiled, created := openTestSession(t, runtimeSession)
	if _, err := connection.RunSession(context.Background(), created); err != nil {
		t.Fatal(err)
	}
	<-started

	if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := connection.RunSession(context.Background(), created); !hasCategory(err, ErrorKindSessionNotFound) {
		t.Fatalf("released plan retained a session: %v", err)
	}

	orderMu.Lock()
	settledOrder := append([]string(nil), order...)
	orderMu.Unlock()
	if !reflect.DeepEqual(settledOrder, []string{"run", "close"}) {
		t.Fatalf("session cleanup was not descendants-first: %#v", settledOrder)
	}

	if runCalls, closeCalls := runtimeSession.counts(); runCalls != 1 || closeCalls != 1 {
		t.Fatalf("unexpected hosted session cleanup: run=%d close=%d", runCalls, closeCalls)
	}
}

func TestSessionLimitCountsPendingAndClosingSessions(t *testing.T) {
	constructorStarted := make(chan struct{})
	finishConstructor := make(chan struct{})
	finishClose := make(chan struct{})
	var constructorOnce sync.Once
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		constructorOnce.Do(func() { close(constructorStarted) })
		<-finishConstructor

		return &spySession{close: func() error {
			<-finishClose

			return nil
		}}, nil
	}}
	host, err := newTestHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, fixtureLimits{
		MaxConnections:                1,
		MaxPlansPerConnection:         1,
		MaxSessionsPerConnection:      1,
		MaxExecutionsPerConnection:    1,
		MaxDebugSessionsPerConnection: 1,
		MaxWatchersPerResource:        1,
		MaxBreakpointsPerDebugSession: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := connection.Compile(context.Background(), compileRequest{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}

	creation := make(chan struct {
		session SessionID
		err     error
	}, 1)
	go func() {
		session, createErr := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID})
		creation <- struct {
			session SessionID
			err     error
		}{session: session, err: createErr}
	}()
	<-constructorStarted

	if _, err := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID}); !hasCategory(err, ErrorKindResourceExhausted) {
		t.Fatalf("pending session did not count against limit: %v", err)
	}
	close(finishConstructor)
	created := <-creation
	if created.err != nil {
		t.Fatal(created.err)
	}

	release := make(chan error, 1)
	go func() { release <- connection.ReleaseSession(context.Background(), created.session) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID}); hasCategory(err, ErrorKindResourceExhausted) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if _, err := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID}); !hasCategory(err, ErrorKindResourceExhausted) {
		t.Fatalf("closing session did not count against limit: %v", err)
	}
	close(finishClose)
	if err := <-release; err != nil {
		t.Fatal(err)
	}

	createdAgain, err := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID})
	if err != nil {
		t.Fatalf("settled session did not release its limit slot: %v", err)
	}

	if err := connection.ReleaseSession(testContext(t), createdAgain); err != nil {
		t.Fatal(err)
	}
}

func openTestSession(t *testing.T, runtimeSession api.Session) (*testEnvironment, planResult, SessionID) {
	t.Helper()
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return runtimeSession, nil
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), compileRequest{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}
	created, err := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}

	return connection, compiled, created
}

func TestSessionCreationFailureDoesNotLeakLimit(t *testing.T) {
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return nil, errors.New("creation failed")
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), compileRequest{Source: api.Source{Content: "RETURN 1"}})
	if err != nil {
		t.Fatal(err)
	}

	for range 2 {
		if _, err := connection.CreateSession(context.Background(), sessionRequest{PlanID: compiled.ID}); !hasCategory(err, ErrorKindInternal) {
			t.Fatalf("unexpected creation failure: %v", err)
		}
	}
}

func TestDurableSessionIsNotReusedAfterRuntimePanic(t *testing.T) {
	runtimeSession := &spySession{run: func(context.Context) (api.Output, error) {
		panic("runtime defect")
	}}
	connection, _, created := openTestSession(t, runtimeSession)
	run, err := connection.RunSession(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	terminal := waitExecution(t, connection, run.ID)
	if terminal.State != wireexecution.StateFailed || terminal.Failure == nil {
		t.Fatalf("runtime panic did not fail the execution: %#v", terminal)
	}

	if err := connection.ReleaseExecution(testContext(t), run.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := connection.RunSession(context.Background(), created); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("poisoned hosted session was reused: %v", err)
	}

	if err := connection.ReleaseSession(testContext(t), created); err != nil {
		t.Fatal(err)
	}

	if runCalls, closeCalls := runtimeSession.counts(); runCalls != 1 || closeCalls != 1 {
		t.Fatalf("unexpected poisoned session cleanup: run=%d close=%d", runCalls, closeCalls)
	}
}

func TestDurableSessionClosePanicSettlesReleaseAndParentCleanup(t *testing.T) {
	runtimeSession := &spySession{close: func() error {
		panic("session close secret")
	}}
	connection, compiled, created := openTestSession(t, runtimeSession)
	if err := connection.ReleaseSession(testContext(t), created); !hasCategory(err, ErrorKindInternal) {
		t.Fatalf("hosted close panic was not retained: %v", err)
	}

	if _, closes := runtimeSession.counts(); closes != 1 {
		t.Fatalf("hosted Session closed %d times", closes)
	}

	if err := connection.ReleaseSession(testContext(t), created); !hasCategory(err, ErrorKindSessionNotFound) {
		t.Fatalf("failed cleanup retained the Session resource: %v", err)
	}

	if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
		t.Fatalf("Session panic blocked parent cleanup: %v", err)
	}
}
