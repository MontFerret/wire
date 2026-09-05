package server_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	hosted.compile = func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
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

func TestRuntimeAdapterCompilePreservesDiagnostics(t *testing.T) {
	values := testDiagnostics()
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
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
