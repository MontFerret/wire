package core

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/MontFerret/api"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestRunUsesBorrowedRuntimeWithoutPlan(t *testing.T) {
	hosted := &spyRuntime{run: func(_ context.Context, _ api.Source, options sessionOptions) (api.Output, error) {
		return api.Output{ContentType: options.contentType, Content: []byte("direct")}, nil
	}}
	connection := newTestConnection(t, hosted)

	run, err := connection.Run(context.Background(), runRequest{
		Source:            api.Source{Name: "direct.fql", Content: "RETURN @input"},
		Parameters:        map[string]any{"input": int64(7)},
		OutputContentType: "text/plain",
	})
	if err != nil {
		t.Fatal(err)
	}

	if run.State != wireexecution.StateRunning {
		t.Fatalf("direct Run did not return the initial running snapshot: %+v", run)
	}

	terminal := waitExecution(t, connection, run.ID)
	if terminal.State != wireexecution.StateCompleted || terminal.Output == nil ||
		terminal.Output.ContentType != "text/plain" || string(terminal.Output.Content) != "direct" {
		t.Fatalf("unexpected direct runtime execution: %#v", terminal)
	}

	if ids := connection.resources.plans; len(ids) != 0 {
		t.Fatalf("direct Runtime.Run created Plans: %#v", ids)
	}

	hosted.mu.Lock()
	sources := append([]api.Source(nil), hosted.runSources...)
	options := make([]sessionOptions, len(hosted.runOptions))
	copy(options, hosted.runOptions)
	hosted.mu.Unlock()

	if !reflect.DeepEqual(sources, []api.Source{{Name: "direct.fql", Content: "RETURN @input"}}) ||
		len(options) != 1 || options[0].contentType != "text/plain" ||
		!reflect.DeepEqual(options[0].params, map[string]any{"input": int64(7)}) {
		t.Fatalf("unexpected Runtime.Run delegation: sources=%#v options=%#v", sources, options)
	}

	if err := connection.ReleaseExecution(testContext(t), run.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRunPanicIsContainedAsInternalFailure(t *testing.T) {
	hosted := &spyRuntime{run: func(context.Context, api.Source, sessionOptions) (api.Output, error) {
		panic("runtime secret")
	}}
	connection := newTestConnection(t, hosted)

	run, err := connection.Run(context.Background(), runRequest{
		Source: api.Source{Content: "RETURN 1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	terminal := waitExecution(t, connection, run.ID)
	if terminal.State != wireexecution.StateFailed || terminal.Output != nil || terminal.Failure == nil ||
		terminal.Failure.Category != failure.CategoryInternalRuntime ||
		terminal.Failure.Message != "runtime operation failed" {
		t.Fatalf("runtime panic crossed the boundary: %#v", terminal)
	}

	if err := connection.ReleaseExecution(testContext(t), run.ID); err != nil {
		t.Fatal(err)
	}

	_, _, closeCalls := hosted.snapshot()
	if closeCalls != 0 {
		t.Fatalf("direct execution closed borrowed Runtime %d times", closeCalls)
	}
}

func TestRunRejectsCancelledAndInvalidRequestsBeforeAllocation(t *testing.T) {
	hosted := &spyRuntime{}
	connection := newTestConnection(t, hosted)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := connection.Run(cancelled, runRequest{Source: api.Source{Content: "RETURN 1"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled direct run was admitted: %v", err)
	}

	if _, err := connection.Run(context.Background(), runRequest{}); !hasCategory(err, ErrorKindInvalidRequest) {
		t.Fatalf("empty direct source was admitted: %v", err)
	}

	if ids := connection.resources.executions; len(ids) != 0 {
		t.Fatalf("rejected direct runs leaked executions: %#v", ids)
	}
}
