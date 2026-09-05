package server_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
)

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
	hosted := &contractRuntime{compile: func(_ context.Context, _ api.Source, debug bool, configured contractPlanOptions) (api.Plan, error) {
		if debug && configured.hasOptimizationLevel && configured.optimizationLevel == api.OptimizationBasic {
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
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
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
