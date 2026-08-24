package client

import (
	"errors"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPlanRunOwnsOnlyItsExecution(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := &clientTestServer{watchScripts: []executionWatchScript{{events: []*wirev1.WatchExecutionResponse{
			executionStartedEvent("execution-1"),
			executionCompletedEvent("execution-1", "text/plain", []byte("done")),
		}}}}
		client := openTestClient(t, startClientTestServer(t, server))
		plan, err := client.Compile(testClientContext(t), Source{Content: "RETURN 1"}, CompileOptions{})
		if err != nil {
			t.Fatal(err)
		}

		output, err := plan.Run(testClientContext(t), nil, ExecuteOptions{})
		if err != nil || string(output.Content) != "done" {
			t.Fatalf("unexpected Plan.Run result: %#v, %v", output, err)
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 0 {
			t.Fatalf("Plan.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}

		extra, err := plan.Execute(testClientContext(t), nil, ExecuteOptions{})
		if err != nil {
			t.Fatalf("Plan.Run closed the caller-owned plan: %v", err)
		}
		if err := plan.Close(testClientContext(t)); err != nil {
			t.Fatal(err)
		}
		if err := extra.Close(testClientContext(t)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("terminal and cleanup failures", func(t *testing.T) {
		server := &clientTestServer{
			watchScripts: []executionWatchScript{{events: []*wirev1.WatchExecutionResponse{
				executionFailedEvent("execution-1", []byte("partial"), &wirev1.Failure{
					Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
					Message:  "execution failed",
				}),
			}}},
			releaseExecutionErr: status.Error(codes.Internal, "execution cleanup failed"),
		}
		client := openTestClient(t, startClientTestServer(t, server))
		plan, err := client.Compile(testClientContext(t), Source{Content: "RETURN 1"}, CompileOptions{})
		if err != nil {
			t.Fatal(err)
		}

		output, err := plan.Run(testClientContext(t), nil, ExecuteOptions{})
		var failure *Failure
		var wireErr *Error
		if string(output.Content) != "partial" || !errors.As(err, &failure) || !errors.As(err, &wireErr) ||
			failure.Message != "execution failed" || wireErr.Message != "execution cleanup failed" {
			t.Fatalf("Plan.Run did not preserve joined errors: %#v, %v", output, err)
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 0 {
			t.Fatalf("Plan.Run failure cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
		if err := plan.Close(testClientContext(t)); err != nil {
			t.Fatal(err)
		}
	})
}
