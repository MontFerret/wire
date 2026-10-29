package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestExecutionStateTerminal(t *testing.T) {
	tests := []struct {
		state    wireexecution.State
		terminal bool
	}{
		{state: 0},
		{state: wireexecution.StateRunning},
		{state: wireexecution.StateCompleted, terminal: true},
		{state: wireexecution.StateFailed, terminal: true},
		{state: wireexecution.StateCancelled, terminal: true},
	}

	for _, test := range tests {
		if got := test.state.Terminal(); got != test.terminal {
			t.Errorf("ExecutionState(%d).Terminal() = %v, want %v", test.state, got, test.terminal)
		}
	}
}

func TestExecutionStateConversionMapsEveryProtocolValue(t *testing.T) {
	tests := []struct {
		protocol wirev1.ExecutionState
		want     wireexecution.State
	}{
		{protocol: wirev1.ExecutionState_EXECUTION_STATE_RUNNING, want: wireexecution.StateRunning},
		{protocol: wirev1.ExecutionState_EXECUTION_STATE_COMPLETED, want: wireexecution.StateCompleted},
		{protocol: wirev1.ExecutionState_EXECUTION_STATE_FAILED, want: wireexecution.StateFailed},
		{protocol: wirev1.ExecutionState_EXECUTION_STATE_CANCELLED, want: wireexecution.StateCancelled},
	}

	for _, test := range tests {
		got, err := convertExecutionState(test.protocol)
		if err != nil {
			t.Fatal(err)
		}

		if got != test.want {
			t.Errorf("convertExecutionState(%v) = %v, want %v", test.protocol, got, test.want)
		}
	}

	if _, err := convertExecutionState(wirev1.ExecutionState(99)); err == nil {
		t.Fatal("unknown execution state was accepted")
	}
}

func TestExecutionConversionDetachesMetadataAndFailure(t *testing.T) {
	protocol := &wirev1.Execution{Id: &wirev1.ExecutionId{Value: "execution-1"}, State: wirev1.ExecutionState_EXECUTION_STATE_FAILED, Failure: &wirev1.Failure{
		Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
		Message:  "runtime operation failed",
		DiagnosticSet: &wirev1.DiagnosticSet{Diagnostics: []*wirev1.Diagnostic{{
			Kind:   "TypeError",
			Source: &wirev1.Source{Name: "query.fql", Content: "RETURN 1"},
			Annotations: []*wirev1.DiagnosticAnnotation{{
				Range: debugTestRange("query.fql", 1, 0, 0, 1),
			}},
		}},
		},
	}}

	protocol.Metadata = &wirev1.OutputMetadata{ContentType: "text/plain"}

	snapshot, err := convertExecutionSnapshot(protocol)
	if err != nil {
		t.Fatal(err)
	}

	protocol.Metadata.ContentType = "changed"

	protocol.Failure.DiagnosticSet.Diagnostics[0].Annotations[0].Message = "changed"
	if snapshot.Metadata == nil || snapshot.Metadata.ContentType != "text/plain" || snapshot.Failure == nil ||
		snapshot.Failure.Category != failure.CategoryExecution ||
		snapshot.Failure.Diagnostics[0].Annotations[0].Message != "" {
		t.Fatalf("execution conversion retained protobuf storage: %#v", snapshot)
	}
}

func TestExecutionOutputTerminalStates(t *testing.T) {
	tests := []struct {
		name        string
		frames      []*wirev1.OutputFrame
		contentType string
		output      string
		failure     bool
		cancelled   bool
	}{
		{
			name:        "completed",
			frames:      completedOutputFrames("application/json", []byte(`{"ok":true}`)),
			contentType: "application/json",
			output:      `{"ok":true}`,
		},
		{
			name: "failed immediately",
			frames: failedOutputFrames([]byte("partial"), &wirev1.Failure{
				Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
				Message:  "remote execution failed",
			}),
			contentType: "text/plain",
			output:      "partial",
			failure:     true,
		},
		{
			name:      "cancelled immediately",
			frames:    []*wirev1.OutputFrame{outputstream.Header(api.Metadata{}), outputstream.End(&wirev1.OutputTerminal{Cancelled: true})},
			cancelled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &clientTestServer{outputScripts: []executionOutputScript{{frames: test.frames}}}
			_, _, execution := openTestExecution(t, server)

			output, err := collectTestExecution(testClientContext(t), execution)
			if test.cancelled && output != nil || !test.cancelled && (output == nil || output.Metadata.ContentType != test.contentType || string(output.Data) != test.output) {
				t.Fatalf("unexpected output: %#v", output)
			}

			switch {
			case test.failure:
				var terminalFailure *failure.Failure
				if !errors.As(err, &terminalFailure) || terminalFailure.Category != failure.CategoryExecution || terminalFailure.Message != "remote execution failed" {
					t.Fatalf("unexpected terminal failure: %#v", err)
				}
			case test.cancelled:
				if !errors.Is(err, ErrExecutionCancelled) || !errors.Is(err, context.Canceled) {
					t.Fatalf("remote cancellation identities were lost: %v", err)
				}
			case err != nil:
				t.Fatal(err)
			}

			_, _, releaseExecutionCalls, _ := server.callSnapshot()
			if releaseExecutionCalls != 1 {
				t.Fatalf("Output consumption released the execution %d times", releaseExecutionCalls)
			}
		})
	}
}

func TestExecutionOutputReleasesHandleAndDetachesOutput(t *testing.T) {
	completed := completedOutputFrames("text/plain", []byte("original"))
	server := &clientTestServer{outputScripts: []executionOutputScript{
		{frames: completed},
	}}
	_, _, execution := openTestExecution(t, server)

	first, err := collectTestExecution(testClientContext(t), execution)
	if err != nil {
		t.Fatal(err)
	}

	first.Data[0] = 'X'

	if _, err := collectTestExecution(testClientContext(t), execution); !errors.Is(err, ErrClosed) {
		t.Fatalf("released output execution was reused: %v", err)
	}

	if string(completed[1].GetChunk()) != "original" {
		t.Fatal("collected bytes alias the server")
	}

	_, outputCalls, releaseExecutionCalls, _ := server.callSnapshot()
	if outputCalls != 1 || releaseExecutionCalls != 1 {
		t.Fatalf("output reads=%d release=%d", outputCalls, releaseExecutionCalls)
	}
}

func TestExecutionOutputPropagatesContextAndStreamErrors(t *testing.T) {
	t.Run("caller cancellation", func(t *testing.T) {
		entered := make(chan struct{})
		server := &clientTestServer{outputScripts: []executionOutputScript{{
			frames:  []*wirev1.OutputFrame{outputstream.Header(api.Metadata{})},
			block:   true,
			entered: entered,
		}}}
		_, _, execution := openTestExecution(t, server)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := collectTestExecution(ctx, execution)
			result <- err
		}()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("Consumption did not attach output delivery")
		}

		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Consumption did not return the caller context error: %#v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Consumption did not observe caller cancellation")
		}
	})

	t.Run("stream failure", func(t *testing.T) {
		server := &clientTestServer{outputScripts: []executionOutputScript{{
			frames: []*wirev1.OutputFrame{outputstream.Header(api.Metadata{})},
			err:    status.Error(codes.Unavailable, "output transport failed"),
		}}}
		_, _, execution := openTestExecution(t, server)

		_, err := collectTestExecution(testClientContext(t), execution)

		var wireErr *Error
		if !errors.As(err, &wireErr) || status.Code(err) != codes.Unavailable || wireErr.Message != "output transport failed" {
			t.Fatalf("unexpected stream failure: %#v", err)
		}
	})
}

func TestExecutionOutputAbsentContent(t *testing.T) {
	server := &clientTestServer{outputScripts: []executionOutputScript{{frames: []*wirev1.OutputFrame{
		outputstream.Header(api.Metadata{}), outputstream.End(&wirev1.OutputTerminal{}),
	}}}}
	_, _, execution := openTestExecution(t, server)

	content, err := collectTestExecution(testClientContext(t), execution)
	if err != nil || content != nil {
		t.Fatalf("absent content=%v error=%v", content, err)
	}
}

func collectTestExecution(ctx context.Context, execution *executionHandle) (*api.Content, error) {
	output, err := execution.openOutput(ctx, nil)
	if err != nil {
		return nil, err
	}

	return output.Collect(ctx)
}

func openTestExecution(t *testing.T, server *clientTestServer) (*connectionHandle, *planHandle, *executionHandle) {
	t.Helper()
	client := openTestClient(t, startClientTestServer(t, server))

	plan, err := client.compileConfigured(testClientContext(t), api.Source{Content: "RETURN 1"}, false, runtimePlanOptions{})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := plan.Close(testClientContext(t)); err != nil {
			t.Errorf("plan cleanup failed: %v", err)
		}
	})

	execution, err := startTestPlanExecution(testClientContext(t), plan, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := execution.Close(testClientContext(t)); err != nil {
			t.Errorf("execution cleanup failed: %v", err)
		}
	})

	return client, plan, execution
}
