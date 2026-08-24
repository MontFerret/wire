package client

import (
	"context"
	"errors"
	"testing"
	"time"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExecutionWaitTerminalStates(t *testing.T) {
	tests := []struct {
		name        string
		events      []*wirev1.WatchExecutionResponse
		contentType string
		output      string
		failure     bool
		cancelled   bool
	}{
		{
			name: "completed after intermediate event",
			events: []*wirev1.WatchExecutionResponse{
				executionStartedEvent("execution-1"),
				executionCompletedEvent("execution-1", "application/json", []byte(`{"ok":true}`)),
			},
			contentType: "application/json",
			output:      `{"ok":true}`,
		},
		{
			name: "failed immediately",
			events: []*wirev1.WatchExecutionResponse{
				executionFailedEvent("execution-1", []byte("partial"), &wirev1.Failure{
					Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
					Message:  "remote execution failed",
				}),
			},
			contentType: "text/plain",
			output:      "partial",
			failure:     true,
		},
		{
			name: "cancelled immediately",
			events: []*wirev1.WatchExecutionResponse{
				executionCancelledEvent("execution-1"),
			},
			cancelled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &clientTestServer{watchScripts: []executionWatchScript{{events: test.events}}}
			_, _, execution := openTestExecution(t, server)

			output, err := execution.Wait(testClientContext(t))
			if output.ContentType != test.contentType || string(output.Content) != test.output {
				t.Fatalf("unexpected output: %#v", output)
			}

			switch {
			case test.failure:
				var failure *Failure
				if !errors.As(err, &failure) || failure.Category != ErrorExecution || failure.Message != "remote execution failed" {
					t.Fatalf("unexpected terminal failure: %#v", err)
				}
			case test.cancelled:
				if !errors.Is(err, ErrExecutionCancelled) || errors.Is(err, context.Canceled) {
					t.Fatalf("remote cancellation was not distinct: %v", err)
				}
			case err != nil:
				t.Fatal(err)
			}

			_, _, releaseExecutionCalls, _ := server.callSnapshot()
			if releaseExecutionCalls != 0 {
				t.Fatalf("Wait released the execution %d times", releaseExecutionCalls)
			}
		})
	}
}

func TestExecutionWaitUsesFreshWatchWithoutCachingOutput(t *testing.T) {
	completed := executionCompletedEvent("execution-1", "text/plain", []byte("original"))
	server := &clientTestServer{watchScripts: []executionWatchScript{
		{events: []*wirev1.WatchExecutionResponse{completed}},
		{events: []*wirev1.WatchExecutionResponse{completed}},
	}}
	_, _, execution := openTestExecution(t, server)

	first, err := execution.Wait(testClientContext(t))
	if err != nil {
		t.Fatal(err)
	}
	first.Content[0] = 'X'
	second, err := execution.Wait(testClientContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if string(second.Content) != "original" {
		t.Fatalf("Wait reused mutable output: %q", second.Content)
	}

	_, watchCalls, releaseExecutionCalls, _ := server.callSnapshot()
	if watchCalls != 2 || releaseExecutionCalls != 0 {
		t.Fatalf("Wait calls: watch=%d release=%d", watchCalls, releaseExecutionCalls)
	}
}

func TestExecutionWaitPropagatesContextAndStreamErrors(t *testing.T) {
	t.Run("caller cancellation", func(t *testing.T) {
		entered := make(chan struct{})
		server := &clientTestServer{watchScripts: []executionWatchScript{{
			events:  []*wirev1.WatchExecutionResponse{executionStartedEvent("execution-1")},
			block:   true,
			entered: entered,
		}}}
		_, _, execution := openTestExecution(t, server)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := execution.Wait(ctx)
			result <- err
		}()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("Wait did not attach its watcher")
		}

		cancel()
		select {
		case err := <-result:
			if err != context.Canceled {
				t.Fatalf("Wait did not return the caller context error: %#v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Wait did not observe caller cancellation")
		}
	})

	t.Run("stream failure", func(t *testing.T) {
		server := &clientTestServer{watchScripts: []executionWatchScript{{
			events: []*wirev1.WatchExecutionResponse{executionStartedEvent("execution-1")},
			err:    status.Error(codes.Unavailable, "watch transport failed"),
		}}}
		_, _, execution := openTestExecution(t, server)

		_, err := execution.Wait(testClientContext(t))
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Code != codes.Unavailable || wireErr.Message != "watch transport failed" {
			t.Fatalf("unexpected stream failure: %#v", err)
		}
	})
}

func TestExecutionWaitRejectsIncompleteTerminalSnapshots(t *testing.T) {
	tests := []struct {
		name    string
		event   *wirev1.WatchExecutionResponse
		message string
	}{
		{
			name: "completed without output",
			event: &wirev1.WatchExecutionResponse{
				ExecutionId: &wirev1.ExecutionId{Value: "execution-1"},
				Sequence:    1,
				Payload: &wirev1.WatchExecutionResponse_Completed{Completed: &wirev1.ExecutionCompleted{
					Execution: executionSnapshotProto("execution-1", wirev1.ExecutionState_EXECUTION_STATE_COMPLETED, nil, nil),
				}},
			},
			message: "Wire server returned a completed execution without output",
		},
		{
			name: "failed without details",
			event: &wirev1.WatchExecutionResponse{
				ExecutionId: &wirev1.ExecutionId{Value: "execution-1"},
				Sequence:    1,
				Payload: &wirev1.WatchExecutionResponse_Failed{Failed: &wirev1.ExecutionFailed{
					Execution: executionSnapshotProto("execution-1", wirev1.ExecutionState_EXECUTION_STATE_FAILED, nil, nil),
				}},
			},
			message: "Wire server returned a failed execution without failure details",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &clientTestServer{watchScripts: []executionWatchScript{{events: []*wirev1.WatchExecutionResponse{test.event}}}}
			_, _, execution := openTestExecution(t, server)

			_, err := execution.Wait(testClientContext(t))
			if err == nil || err.Error() != test.message {
				t.Fatalf("unexpected incomplete-terminal result: %v", err)
			}
		})
	}
}

func openTestExecution(t *testing.T, server *clientTestServer) (*Client, *Plan, *Execution) {
	t.Helper()
	client := openTestClient(t, startClientTestServer(t, server))
	plan, err := client.Compile(testClientContext(t), Source{Content: "RETURN 1"}, CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := plan.Close(testClientContext(t)); err != nil {
			t.Errorf("plan cleanup failed: %v", err)
		}
	})

	execution, err := plan.Execute(testClientContext(t), nil, ExecuteOptions{})
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

func executionSnapshotProto(id string, state wirev1.ExecutionState, output *wirev1.Output, failure *wirev1.Failure) *wirev1.Execution {
	return &wirev1.Execution{
		Id:      &wirev1.ExecutionId{Value: id},
		PlanId:  &wirev1.PlanId{Value: "plan-1"},
		State:   state,
		Output:  output,
		Failure: failure,
	}
}

func executionStartedEvent(id string) *wirev1.WatchExecutionResponse {
	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    1,
		Payload: &wirev1.WatchExecutionResponse_Started{Started: &wirev1.ExecutionStarted{
			Execution: executionSnapshotProto(id, wirev1.ExecutionState_EXECUTION_STATE_RUNNING, nil, nil),
		}},
	}
}

func executionCompletedEvent(id string, contentType string, content []byte) *wirev1.WatchExecutionResponse {
	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    2,
		Payload: &wirev1.WatchExecutionResponse_Completed{Completed: &wirev1.ExecutionCompleted{
			Execution: executionSnapshotProto(id, wirev1.ExecutionState_EXECUTION_STATE_COMPLETED, &wirev1.Output{
				ContentType: contentType,
				Content:     append([]byte(nil), content...),
			}, nil),
		}},
	}
}

func executionFailedEvent(id string, content []byte, failure *wirev1.Failure) *wirev1.WatchExecutionResponse {
	var output *wirev1.Output
	if content != nil {
		output = &wirev1.Output{ContentType: "text/plain", Content: append([]byte(nil), content...)}
	}

	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    2,
		Payload: &wirev1.WatchExecutionResponse_Failed{Failed: &wirev1.ExecutionFailed{
			Execution: executionSnapshotProto(id, wirev1.ExecutionState_EXECUTION_STATE_FAILED, output, failure),
		}},
	}
}

func executionCancelledEvent(id string) *wirev1.WatchExecutionResponse {
	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    2,
		Payload: &wirev1.WatchExecutionResponse_Cancelled{Cancelled: &wirev1.ExecutionCancelled{
			Execution: executionSnapshotProto(id, wirev1.ExecutionState_EXECUTION_STATE_CANCELLED, nil, nil),
		}},
	}
}
