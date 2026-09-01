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
)

func TestHostRejectsNilRuntimeAndDoesNotCloseBorrowedRuntime(t *testing.T) {
	var typedNil *spyRuntime
	for name, runtime := range map[string]api.Runtime{
		"nil interface": nil,
		"typed nil":     typedNil,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewHost(runtime, RuntimeInfo{}, testLimits()); !hasCategory(err, ErrorInvalidRequest) {
				t.Fatalf("unexpected nil runtime result: %v", err)
			}
		})
	}

	runtime := &spyRuntime{}
	host, err := NewHost(runtime, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.OpenConnection(); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}

	_, _, closeCalls := runtime.snapshot()
	if closeCalls != 0 {
		t.Fatalf("host closed borrowed runtime %d times", closeCalls)
	}
}

func TestCompileExecuteRetainsReusableAPIPlanAndSessionOptions(t *testing.T) {
	outputBytes := []byte(`{"ok":true}`)
	var sessionsMu sync.Mutex
	var sessions []*spySession
	plan := &spyPlan{
		params: []string{"input"},
		newSession: func(context.Context, sessionOptions) (api.Session, error) {
			session := &spySession{run: func(context.Context) (api.Output, error) {
				return api.Output{ContentType: "application/json", Content: outputBytes}, nil
			}}
			sessionsMu.Lock()
			sessions = append(sessions, session)
			sessionsMu.Unlock()

			return session, nil
		},
	}
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

	compiled, err := connection.Compile(context.Background(), CompileInput{
		Source: api.Source{
			Name:    "reusable.fql",
			Content: "RETURN @input",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(compiled.Parameters, []string{"input"}) {
		t.Fatalf("unexpected parameters: %#v", compiled.Parameters)
	}

	connection.mu.RLock()
	retained := connection.plans[compiled.ID]
	connection.mu.RUnlock()
	if retained == nil || retained.plan != plan {
		t.Fatal("Wire plan did not retain the API plan")
	}
	plan.mu.Lock()
	plan.params[0] = "changed by runtime"
	plan.mu.Unlock()
	if got := retained.snapshot().Parameters; !reflect.DeepEqual(got, []string{"input"}) {
		t.Fatalf("Wire plan retained runtime parameter storage: %#v", got)
	}

	inputs := []ExecuteInput{
		{PlanID: compiled.ID, Parameters: map[string]any{"input": []any{int64(1), []byte{2}}}},
		{PlanID: compiled.ID, Parameters: map[string]any{"input": "second"}, OutputContentType: "application/x-wire"},
	}
	for _, input := range inputs {
		execution, executeErr := connection.Execute(context.Background(), input)
		if executeErr != nil {
			t.Fatal(executeErr)
		}
		finished := waitExecution(t, connection, execution.ID)
		if finished.State != ExecutionCompleted || finished.Output == nil || finished.Output.ContentType != "application/json" {
			t.Fatalf("unexpected execution result: %#v", finished)
		}
	}

	outputBytes[0] = '!'
	connection.mu.RLock()
	for id := range retained.executions {
		execution := connection.executions[id]
		if execution == nil {
			continue
		}
		if got := execution.snapshot().Output.Content[0]; got != '{' {
			t.Fatalf("execution retained runtime output storage: %q", got)
		}
	}
	connection.mu.RUnlock()

	options, _, _ := plan.snapshot()
	if len(options) != 2 {
		t.Fatalf("expected two reusable sessions, got %d", len(options))
	}
	if !reflect.DeepEqual(options[0].params, inputs[0].Parameters) || options[0].contentType != "" {
		t.Fatalf("unexpected first options: %#v", options[0])
	}
	if !reflect.DeepEqual(options[1].params, inputs[1].Parameters) || options[1].contentType != "application/x-wire" {
		t.Fatalf("unexpected second options: %#v", options[1])
	}

	sessionsMu.Lock()
	createdSessions := append([]*spySession(nil), sessions...)
	sessionsMu.Unlock()
	for _, session := range createdSessions {
		runCalls, closeCalls := session.counts()
		if runCalls != 1 || closeCalls != 1 {
			t.Fatalf("unexpected session lifecycle: run=%d close=%d", runCalls, closeCalls)
		}
	}

	if err := connection.ReleasePlan(testContext(t), compiled.ID); err != nil {
		t.Fatal(err)
	}
	_, _, planCloseCalls := plan.snapshot()
	if planCloseCalls != 1 {
		t.Fatalf("API plan closed %d times", planCloseCalls)
	}
}

func TestCompileDelegatesDebugSelectionAndClosesAbandonedPlan(t *testing.T) {
	compiled := &spyPlan{}
	runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return compiled, nil
	}}
	host, err := NewHost(runtime, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := connection.Compile(ctx, CompileInput{Source: api.Source{Name: "debug.fql", Content: "RETURN 1"}, Debuggable: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancelled compile result: %v", err)
	}

	plan, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Name: "debug.fql", Content: "RETURN 1"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	sources, debug, _ := runtime.snapshot()
	if len(sources) != 1 || sources[0] != (api.Source{Name: "debug.fql", Content: "RETURN 1"}) || !debug[0] {
		t.Fatalf("unexpected compile delegation: %#v %#v", sources, debug)
	}
	if err := connection.ReleasePlan(testContext(t), plan.ID); err != nil {
		t.Fatal(err)
	}
	runtime.compile = func(context.Context, api.Source, bool) (api.Plan, error) {
		return &spyPlan{}, nil
	}
	anonymous, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 2"}})
	if err != nil {
		t.Fatal(err)
	}
	sources, debug, _ = runtime.snapshot()
	if len(sources) != 2 || sources[1] != (api.Source{Name: "anonymous", Content: "RETURN 2"}) || debug[1] {
		t.Fatalf("unexpected anonymous source delegation: %#v %#v", sources, debug)
	}
	if err := connection.ReleasePlan(testContext(t), anonymous.ID); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	abandoned := &spyPlan{}
	runtime.compile = func(ctx context.Context, _ api.Source, _ bool) (api.Plan, error) {
		close(started)
		<-release

		return abandoned, nil
	}
	compileCtx, cancelCompile := context.WithCancel(context.Background())
	compileResult := make(chan error, 1)
	go func() {
		_, compileErr := connection.Compile(compileCtx, CompileInput{Source: api.Source{Content: "RETURN 2"}})
		compileResult <- compileErr
	}()
	<-started
	cancelCompile()
	close(release)
	if err := <-compileResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected abandoned compile result: %v", err)
	}
	_, _, closeCalls := abandoned.snapshot()
	if closeCalls != 1 {
		t.Fatalf("abandoned API plan closed %d times", closeCalls)
	}
}

func TestCompileForwardsOptionalOptimizationLevel(t *testing.T) {
	runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return &spyPlan{}, nil
	}}
	host, err := NewHost(runtime, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	levels := []*api.OptimizationLevel{
		nil,
		optimizationLevel(api.OptimizationNone),
		optimizationLevel(api.OptimizationBasic),
		optimizationLevel(api.OptimizationFull),
		optimizationLevel(api.OptimizationAggressive),
	}
	for index, level := range levels {
		compiled, compileErr := connection.Compile(context.Background(), CompileInput{
			Source:            api.Source{Content: "RETURN 1"},
			OptimizationLevel: level,
		})
		if compileErr != nil {
			t.Fatalf("compile %d failed: %v", index, compileErr)
		}

		if releaseErr := connection.ReleasePlan(testContext(t), compiled.ID); releaseErr != nil {
			t.Fatalf("release %d failed: %v", index, releaseErr)
		}
	}

	got := runtime.optimizationSnapshot()
	if len(got) != len(levels) {
		t.Fatalf("recorded %d optimization values, want %d", len(got), len(levels))
	}
	for index, want := range levels {
		if want == nil {
			if got[index] != nil {
				t.Fatalf("unspecified optimization forwarded as %v", *got[index])
			}

			continue
		}

		if got[index] == nil || *got[index] != *want {
			t.Fatalf("optimization %d = %v, want %v", index, got[index], *want)
		}
	}
}

func optimizationLevel(value api.OptimizationLevel) *api.OptimizationLevel {
	return &value
}

func TestCompilePanicsAreSanitizedAndCloseReturnedPlansOnce(t *testing.T) {
	t.Run("compile", func(t *testing.T) {
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			panic("compile secret")
		}})

		if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}}); !hasCategory(err, ErrorInternal) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("compile panic was not sanitized: %v", err)
		}
	})

	t.Run("metadata", func(t *testing.T) {
		plan := &spyPlan{paramsCall: func() []string { panic("metadata secret") }}
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return plan, nil
		}})

		if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}}); !hasCategory(err, ErrorInternal) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("metadata panic was not sanitized: %v", err)
		}

		_, _, closeCalls := plan.snapshot()
		if closeCalls != 1 {
			t.Fatalf("plan with panicking metadata closed %d times", closeCalls)
		}
	})

	t.Run("abandoned cleanup", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		plan := &spyPlan{close: func() error { panic("close secret") }}
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			cancel()

			return plan, nil
		}})

		if _, err := connection.Compile(ctx, CompileInput{Source: api.Source{Content: "RETURN 1"}}); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("abandoned cleanup panic was not contained: %v", err)
		}

		_, _, closeCalls := plan.snapshot()
		if closeCalls != 1 {
			t.Fatalf("abandoned plan cleanup attempted %d times", closeCalls)
		}
	})
}

func TestSuccessfulNilAPIResourcesAreRejectedSafely(t *testing.T) {
	var nilPlan *spyPlan
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return nilPlan, nil
	}})
	if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}}); !hasCategory(err, ErrorInternal) {
		t.Fatalf("typed-nil plan was not rejected: %v", err)
	}

	var nilSession *spySession
	plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return nilSession, nil
	}}
	connection = newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
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
		t.Fatalf("typed-nil session was not rejected: %#v", finished)
	}
}

func TestAPIResourcesReturnedWithErrorsAreClosedOnce(t *testing.T) {
	runtimeErr := errors.New("runtime secret")

	t.Run("plan", func(t *testing.T) {
		plan := &spyPlan{}
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return plan, runtimeErr
		}})

		if _, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}}); !hasCategory(err, ErrorCompilation) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unexpected plan-plus-error result: %v", err)
		}

		_, _, closeCalls := plan.snapshot()
		if closeCalls != 1 {
			t.Fatalf("plan returned with an error closed %d times", closeCalls)
		}
	})

	t.Run("session", func(t *testing.T) {
		session := &spySession{}
		plan := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
			return session, runtimeErr
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
		if finished.State != ExecutionFailed || finished.Failure == nil || finished.Failure.Category != ErrorInternal ||
			strings.Contains(finished.Failure.Message, "secret") {
			t.Fatalf("unexpected session-plus-error result: %#v", finished)
		}

		_, closeCalls := session.counts()
		if closeCalls != 1 {
			t.Fatalf("session returned with an error closed %d times", closeCalls)
		}
	})

	t.Run("debug session", func(t *testing.T) {
		debugSession := &spyDebugger{}
		plan := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
			return debugSession, runtimeErr
		}}
		connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return plan, nil
		}})
		compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}, Debuggable: true})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := connection.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID}); !hasCategory(err, ErrorInternal) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unexpected debug-session-plus-error result: %v", err)
		}

		if closeCalls := debugSession.closes(); closeCalls != 1 {
			t.Fatalf("debug session returned with an error closed %d times", closeCalls)
		}
	})
}

func TestAbandonedDebugSessionCleanupPanicIsSanitized(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	debugSession := &spyDebugger{close: func() error { panic("debug close secret") }}
	plan := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
		cancel()

		return debugSession, nil
	}}
	connection := newTestConnection(t, &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}})
	compiled, err := connection.Compile(context.Background(), CompileInput{Source: api.Source{Content: "RETURN 1"}, Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.OpenDebugSession(ctx, OpenDebugInput{PlanID: compiled.ID}); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("abandoned debug cleanup panic was not contained: %v", err)
	}

	if closeCalls := debugSession.closes(); closeCalls != 1 {
		t.Fatalf("abandoned debug cleanup attempted %d times", closeCalls)
	}
}

func TestExecutionUsesPortableFailureFallbacks(t *testing.T) {
	runSecret := errors.New("run secret must not escape")
	closeSecret := errors.New("close secret must not escape")
	tests := []struct {
		name       string
		runErr     error
		closeErr   error
		want       ErrorCategory
		wantOutput bool
	}{
		{name: "run", runErr: runSecret, want: ErrorExecution, wantOutput: true},
		{name: "close", closeErr: closeSecret, want: ErrorInternal, wantOutput: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &spySession{
				run: func(context.Context) (api.Output, error) {
					return api.Output{ContentType: "application/json", Content: []byte("1")}, test.runErr
				},
				close: func() error { return test.closeErr },
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
			if finished.State != ExecutionFailed || finished.Failure == nil || finished.Failure.Category != test.want {
				t.Fatalf("unexpected failure: %#v", finished)
			}
			if (finished.Output != nil) != test.wantOutput {
				t.Fatalf("unexpected output presence: %#v", finished.Output)
			}
			if strings.Contains(finished.Failure.Message, "secret") {
				t.Fatalf("runtime detail leaked: %#v", finished.Failure)
			}
			runCalls, closeCalls := session.counts()
			if runCalls != 1 || closeCalls != 1 {
				t.Fatalf("unexpected session lifecycle: run=%d close=%d", runCalls, closeCalls)
			}
		})
	}
}

func newTestConnection(t *testing.T, runtime api.Runtime) *Connection {
	t.Helper()
	host, err := NewHost(runtime, RuntimeInfo{}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	return connection
}

func waitExecution(t *testing.T, connection *Connection, id ExecutionID) ExecutionSnapshot {
	t.Helper()
	execution, err := connection.execution(id)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-execution.done:
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not finish")
	}

	return execution.snapshot()
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	return ctx
}

func testLimits() Limits {
	return Limits{
		MaxConnections:                4,
		MaxPlansPerConnection:         4,
		MaxExecutionsPerConnection:    4,
		MaxDebugSessionsPerConnection: 4,
		MaxWatchersPerResource:        4,
		MaxBreakpointsPerDebugSession: 4,
	}
}

func hasCategory(err error, category ErrorCategory) bool {
	var domain *DomainError

	return errors.As(err, &domain) && domain.Category == category
}
