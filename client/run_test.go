package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type (
	runWatchScript struct {
		events  []*wirev1.WatchExecutionResponse
		err     error
		block   bool
		entered chan struct{}
	}

	runServer struct {
		wirev1.UnimplementedRuntimeServiceServer
		wirev1.UnimplementedPlanServiceServer
		wirev1.UnimplementedExecutionServiceServer

		mu                    sync.Mutex
		plans                 int
		executions            int
		calls                 []string
		compileErr            error
		executeErr            error
		releaseExecutionErr   error
		releasePlanErr        error
		watchScripts          []runWatchScript
		watchCalls            int
		lastCompileDebuggable bool
		lastOutputContentType string
		releaseExecutionCalls int
		releasePlanCalls      int
	}
)

func (s *runServer) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	if err := stream.Send(&wirev1.ConnectResponse{Opened: &wirev1.ConnectionOpened{
		ConnectionId: &wirev1.ConnectionId{Value: "connection"},
		RuntimeInfo:  &wirev1.RuntimeInfo{ApiIdentity: "ferret.wire.v1"},
	}}); err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *runServer) CloseConnection(_ context.Context, request *wirev1.CloseConnectionRequest) (*wirev1.CloseConnectionResponse, error) {
	s.record("close-connection", request.GetConnectionId().GetValue(), "")

	return &wirev1.CloseConnectionResponse{}, nil
}

func (s *runServer) Compile(_ context.Context, request *wirev1.CompileRequest) (*wirev1.CompileResponse, error) {
	s.mu.Lock()
	s.calls = append(s.calls, call("compile", request.GetConnectionId().GetValue(), ""))
	s.lastCompileDebuggable = request.GetOptions().GetDebuggable()
	err := s.compileErr
	if err == nil {
		s.plans++
	}
	planID := fmt.Sprintf("plan-%d", s.plans)
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &wirev1.CompileResponse{Plan: &wirev1.Plan{Id: &wirev1.PlanId{Value: planID}}}, nil
}

func (s *runServer) ReleasePlan(_ context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
	s.mu.Lock()
	s.releasePlanCalls++
	s.calls = append(s.calls, call("release-plan", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue()))
	err := s.releasePlanErr
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &wirev1.ReleasePlanResponse{}, nil
}

func (s *runServer) Execute(_ context.Context, request *wirev1.ExecuteRequest) (*wirev1.ExecuteResponse, error) {
	s.mu.Lock()
	s.calls = append(s.calls, call("execute", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue()))
	s.lastOutputContentType = request.GetOutputContentType()
	err := s.executeErr
	if err == nil {
		s.executions++
	}
	executionID := fmt.Sprintf("execution-%d", s.executions)
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &wirev1.ExecuteResponse{Execution: runExecution(executionID, wirev1.ExecutionState_EXECUTION_STATE_RUNNING, nil, nil)}, nil
}

func (s *runServer) ReleaseExecution(_ context.Context, request *wirev1.ReleaseExecutionRequest) (*wirev1.ReleaseExecutionResponse, error) {
	s.mu.Lock()
	s.releaseExecutionCalls++
	s.calls = append(s.calls, call("release-execution", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue()))
	err := s.releaseExecutionErr
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &wirev1.ReleaseExecutionResponse{}, nil
}

func (s *runServer) WatchExecution(request *wirev1.WatchExecutionRequest, stream wirev1.ExecutionService_WatchExecutionServer) error {
	s.mu.Lock()
	s.calls = append(s.calls, call("watch", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue()))
	index := s.watchCalls
	s.watchCalls++
	var script runWatchScript
	if index < len(s.watchScripts) {
		script = s.watchScripts[index]
	}
	s.mu.Unlock()

	for _, event := range script.events {
		if err := stream.Send(event); err != nil {
			return err
		}
	}

	if script.entered != nil {
		close(script.entered)
	}

	if script.block {
		<-stream.Context().Done()

		return stream.Context().Err()
	}

	return script.err
}

func (s *runServer) record(name string, connectionID string, resourceID string) {
	s.mu.Lock()
	s.calls = append(s.calls, call(name, connectionID, resourceID))
	s.mu.Unlock()
}

func (s *runServer) snapshot() ([]string, int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.calls...), s.watchCalls, s.releaseExecutionCalls, s.releasePlanCalls
}

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
				runStartedEvent("execution-1"),
				runCompletedEvent("execution-1", "application/json", []byte(`{"ok":true}`)),
			},
			contentType: "application/json",
			output:      `{"ok":true}`,
		},
		{
			name: "failed immediately",
			events: []*wirev1.WatchExecutionResponse{
				runFailedEvent("execution-1", []byte("partial"), &wirev1.Failure{
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
				runCancelledEvent("execution-1"),
			},
			cancelled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &runServer{watchScripts: []runWatchScript{{events: test.events}}}
			_, _, execution := openRunExecution(t, server)

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

			_, _, releaseExecutionCalls, _ := server.snapshot()
			if releaseExecutionCalls != 0 {
				t.Fatalf("Wait released the execution %d times", releaseExecutionCalls)
			}
		})
	}
}

func TestExecutionWaitUsesFreshWatchWithoutCachingOutput(t *testing.T) {
	completed := runCompletedEvent("execution-1", "text/plain", []byte("original"))
	server := &runServer{watchScripts: []runWatchScript{{events: []*wirev1.WatchExecutionResponse{completed}}, {events: []*wirev1.WatchExecutionResponse{completed}}}}
	_, _, execution := openRunExecution(t, server)

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

	_, watchCalls, releaseExecutionCalls, _ := server.snapshot()
	if watchCalls != 2 || releaseExecutionCalls != 0 {
		t.Fatalf("Wait calls: watch=%d release=%d", watchCalls, releaseExecutionCalls)
	}
}

func TestExecutionWaitPropagatesContextAndStreamErrors(t *testing.T) {
	t.Run("caller cancellation", func(t *testing.T) {
		entered := make(chan struct{})
		server := &runServer{watchScripts: []runWatchScript{{
			events:  []*wirev1.WatchExecutionResponse{runStartedEvent("execution-1")},
			block:   true,
			entered: entered,
		}}}
		_, _, execution := openRunExecution(t, server)
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
		server := &runServer{watchScripts: []runWatchScript{{
			events: []*wirev1.WatchExecutionResponse{runStartedEvent("execution-1")},
			err:    status.Error(codes.Unavailable, "watch transport failed"),
		}}}
		_, _, execution := openRunExecution(t, server)

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
					Execution: runExecution("execution-1", wirev1.ExecutionState_EXECUTION_STATE_COMPLETED, nil, nil),
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
					Execution: runExecution("execution-1", wirev1.ExecutionState_EXECUTION_STATE_FAILED, nil, nil),
				}},
			},
			message: "Wire server returned a failed execution without failure details",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &runServer{watchScripts: []runWatchScript{{events: []*wirev1.WatchExecutionResponse{test.event}}}}
			_, _, execution := openRunExecution(t, server)

			_, err := execution.Wait(testClientContext(t))
			if err == nil || err.Error() != test.message {
				t.Fatalf("unexpected incomplete-terminal result: %v", err)
			}
		})
	}
}

func TestPlanRunOwnsOnlyItsExecution(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		server := &runServer{watchScripts: []runWatchScript{{events: []*wirev1.WatchExecutionResponse{
			runStartedEvent("execution-1"),
			runCompletedEvent("execution-1", "text/plain", []byte("done")),
		}}}}
		client := openRunClient(t, startRunServer(t, server))
		plan, err := client.Compile(testClientContext(t), Source{Content: "RETURN 1"}, CompileOptions{})
		if err != nil {
			t.Fatal(err)
		}

		output, err := plan.Run(testClientContext(t), nil, ExecuteOptions{})
		if err != nil || string(output.Content) != "done" {
			t.Fatalf("unexpected Plan.Run result: %#v, %v", output, err)
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.snapshot()
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
		server := &runServer{
			watchScripts: []runWatchScript{{events: []*wirev1.WatchExecutionResponse{
				runFailedEvent("execution-1", []byte("partial"), &wirev1.Failure{
					Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
					Message:  "execution failed",
				}),
			}}},
			releaseExecutionErr: status.Error(codes.Internal, "execution cleanup failed"),
		}
		client := openRunClient(t, startRunServer(t, server))
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
		_, _, releaseExecutionCalls, releasePlanCalls := server.snapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 0 {
			t.Fatalf("Plan.Run failure cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
		if err := plan.Close(testClientContext(t)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClientRunOwnsCreatedResources(t *testing.T) {
	t.Run("success and options", func(t *testing.T) {
		server := &runServer{watchScripts: []runWatchScript{{events: []*wirev1.WatchExecutionResponse{
			runCompletedEvent("execution-1", "application/json", []byte(`{"value":1}`)),
		}}}}
		client := openRunClient(t, startRunServer(t, server))

		output, err := client.Run(testClientContext(t), Source{Content: "RETURN 1"}, nil, RunOptions{
			Compile: CompileOptions{Debuggable: true},
			Execute: ExecuteOptions{OutputContentType: "application/json"},
		})
		if err != nil || string(output.Content) != `{"value":1}` {
			t.Fatalf("unexpected Client.Run result: %#v, %v", output, err)
		}

		calls, _, releaseExecutionCalls, releasePlanCalls := server.snapshot()
		want := []string{
			call("compile", "connection", ""),
			call("execute", "connection", "plan-1"),
			call("watch", "connection", "execution-1"),
			call("release-execution", "connection", "execution-1"),
			call("release-plan", "connection", "plan-1"),
		}
		server.mu.Lock()
		debuggable := server.lastCompileDebuggable
		contentType := server.lastOutputContentType
		server.mu.Unlock()
		if !slices.Equal(calls, want) || !debuggable || contentType != "application/json" || releaseExecutionCalls != 1 || releasePlanCalls != 1 {
			t.Fatalf("Client.Run orchestration: calls=%v debug=%v content=%q releases=%d/%d", calls, debuggable, contentType, releaseExecutionCalls, releasePlanCalls)
		}
	})

	t.Run("compile failure creates nothing", func(t *testing.T) {
		server := &runServer{compileErr: status.Error(codes.InvalidArgument, "compile failed")}
		client := openRunClient(t, startRunServer(t, server))

		_, err := client.Run(testClientContext(t), Source{Content: "invalid"}, nil, RunOptions{})
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Message != "compile failed" {
			t.Fatalf("unexpected compile failure: %v", err)
		}
		calls, watchCalls, releaseExecutionCalls, releasePlanCalls := server.snapshot()
		if !slices.Equal(calls, []string{call("compile", "connection", "")}) || watchCalls != 0 || releaseExecutionCalls != 0 || releasePlanCalls != 0 {
			t.Fatalf("compile failure leaked cleanup calls: %v", calls)
		}
	})

	t.Run("execute failure releases plan", func(t *testing.T) {
		server := &runServer{executeErr: status.Error(codes.InvalidArgument, "execute failed")}
		client := openRunClient(t, startRunServer(t, server))

		_, err := client.Run(testClientContext(t), Source{Content: "RETURN 1"}, nil, RunOptions{})
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Message != "execute failed" {
			t.Fatalf("unexpected execute failure: %v", err)
		}
		calls, watchCalls, releaseExecutionCalls, releasePlanCalls := server.snapshot()
		want := []string{
			call("compile", "connection", ""),
			call("execute", "connection", "plan-1"),
			call("release-plan", "connection", "plan-1"),
		}
		if !slices.Equal(calls, want) || watchCalls != 0 || releaseExecutionCalls != 0 || releasePlanCalls != 1 {
			t.Fatalf("execute failure cleanup: %v", calls)
		}
	})

	t.Run("caller cancellation still cleans up", func(t *testing.T) {
		entered := make(chan struct{})
		server := &runServer{watchScripts: []runWatchScript{{
			events:  []*wirev1.WatchExecutionResponse{runStartedEvent("execution-1")},
			block:   true,
			entered: entered,
		}}}
		client := openRunClient(t, startRunServer(t, server))
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := client.Run(ctx, Source{Content: "RETURN 1"}, nil, RunOptions{})
			result <- err
		}()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("Client.Run did not begin waiting")
		}

		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Client.Run lost caller cancellation: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Client.Run cleanup did not settle")
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.snapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 1 {
			t.Fatalf("cancelled Client.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
	})

	t.Run("stream failure still cleans up", func(t *testing.T) {
		server := &runServer{watchScripts: []runWatchScript{{
			events: []*wirev1.WatchExecutionResponse{runStartedEvent("execution-1")},
			err:    status.Error(codes.Unavailable, "watch transport failed"),
		}}}
		client := openRunClient(t, startRunServer(t, server))

		_, err := client.Run(testClientContext(t), Source{Content: "RETURN 1"}, nil, RunOptions{})
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Code != codes.Unavailable || wireErr.Message != "watch transport failed" {
			t.Fatalf("Client.Run lost the stream failure: %v", err)
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.snapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 1 {
			t.Fatalf("stream-failed Client.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
	})

	t.Run("execution and cleanup failures are joined", func(t *testing.T) {
		server := &runServer{
			watchScripts: []runWatchScript{{events: []*wirev1.WatchExecutionResponse{
				runFailedEvent("execution-1", []byte("partial"), &wirev1.Failure{
					Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
					Message:  "execution failed",
				}),
			}}},
			releaseExecutionErr: status.Error(codes.Internal, "execution cleanup failed"),
			releasePlanErr:      status.Error(codes.Unavailable, "plan cleanup failed"),
		}
		client := openRunClient(t, startRunServer(t, server))

		output, err := client.Run(testClientContext(t), Source{Content: "RETURN 1"}, nil, RunOptions{})
		var failure *Failure
		if string(output.Content) != "partial" || !errors.As(err, &failure) || failure.Message != "execution failed" ||
			!strings.Contains(err.Error(), "execution cleanup failed") || !strings.Contains(err.Error(), "plan cleanup failed") {
			t.Fatalf("Client.Run did not preserve all errors: %#v, %v", output, err)
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.snapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 1 {
			t.Fatalf("failed Client.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
	})
}

func openRunExecution(t *testing.T, server *runServer) (*Client, *Plan, *Execution) {
	t.Helper()
	client := openRunClient(t, startRunServer(t, server))
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

func openRunClient(t *testing.T, connection grpc.ClientConnInterface) *Client {
	t.Helper()
	client, err := New(testClientContext(t), connection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(testClientContext(t)); err != nil {
			t.Errorf("client cleanup failed: %v", err)
		}
	})

	return client
}

func startRunServer(t *testing.T, implementation *runServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	wirev1.RegisterRuntimeServiceServer(server, implementation)
	wirev1.RegisterPlanServiceServer(server, implementation)
	wirev1.RegisterExecutionServiceServer(server, implementation)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///client-run-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		server.Stop()
		if err := connection.Close(); err != nil {
			t.Errorf("transport cleanup failed: %v", err)
		}

		if err := listener.Close(); err != nil {
			t.Errorf("listener cleanup failed: %v", err)
		}

		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("server cleanup failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("server cleanup did not settle")
		}
	})

	return connection
}

func runExecution(id string, state wirev1.ExecutionState, output *wirev1.Output, failure *wirev1.Failure) *wirev1.Execution {
	return &wirev1.Execution{
		Id:      &wirev1.ExecutionId{Value: id},
		PlanId:  &wirev1.PlanId{Value: "plan-1"},
		State:   state,
		Output:  output,
		Failure: failure,
	}
}

func runStartedEvent(id string) *wirev1.WatchExecutionResponse {
	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    1,
		Payload: &wirev1.WatchExecutionResponse_Started{Started: &wirev1.ExecutionStarted{
			Execution: runExecution(id, wirev1.ExecutionState_EXECUTION_STATE_RUNNING, nil, nil),
		}},
	}
}

func runCompletedEvent(id string, contentType string, content []byte) *wirev1.WatchExecutionResponse {
	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    2,
		Payload: &wirev1.WatchExecutionResponse_Completed{Completed: &wirev1.ExecutionCompleted{
			Execution: runExecution(id, wirev1.ExecutionState_EXECUTION_STATE_COMPLETED, &wirev1.Output{
				ContentType: contentType,
				Content:     append([]byte(nil), content...),
			}, nil),
		}},
	}
}

func runFailedEvent(id string, content []byte, failure *wirev1.Failure) *wirev1.WatchExecutionResponse {
	var output *wirev1.Output
	if content != nil {
		output = &wirev1.Output{ContentType: "text/plain", Content: append([]byte(nil), content...)}
	}

	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    2,
		Payload: &wirev1.WatchExecutionResponse_Failed{Failed: &wirev1.ExecutionFailed{
			Execution: runExecution(id, wirev1.ExecutionState_EXECUTION_STATE_FAILED, output, failure),
		}},
	}
}

func runCancelledEvent(id string) *wirev1.WatchExecutionResponse {
	return &wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    2,
		Payload: &wirev1.WatchExecutionResponse_Cancelled{Cancelled: &wirev1.ExecutionCancelled{
			Execution: runExecution(id, wirev1.ExecutionState_EXECUTION_STATE_CANCELLED, nil, nil),
		}},
	}
}
