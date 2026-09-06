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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/failure"
)

type (
	executionWatchScript struct {
		events  []*wirev1.WatchExecutionResponse
		err     error
		block   bool
		entered chan struct{}
	}

	clientTestServer struct {
		wirev1.UnimplementedRuntimeServiceServer
		wirev1.UnimplementedPlanServiceServer
		wirev1.UnimplementedExecutionServiceServer
		wirev1.UnimplementedSessionServiceServer

		mu                    sync.Mutex
		handshake             *wirev1.ConnectResponse
		connectErr            error
		connectDone           chan struct{}
		plans                 int
		sessions              int
		executions            int
		calls                 []string
		compileErr            error
		executeErr            error
		releaseExecutionErr   error
		releasePlanErr        error
		watchScripts          []executionWatchScript
		watchCalls            int
		lastCompileDebuggable bool
		lastCompileSourceName string
		lastCompileContent    string
		lastOutputContentType string
		releaseExecutionCalls int
		releasePlanCalls      int
		releaseExecutionLimit time.Time
		releasePlanLimit      time.Time
	}
)

func (s *clientTestServer) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	if s.connectDone != nil {
		defer close(s.connectDone)
	}

	if s.connectErr != nil {
		return s.connectErr
	}

	response := s.handshake
	if response == nil {
		response = &wirev1.ConnectResponse{
			ConnectionId: &wirev1.ConnectionId{Value: "connection"},
			Protocol:     &wirev1.ProtocolInfo{Name: "ferret.wire", Version: "v1"},
		}
	}

	if err := stream.Send(response); err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *clientTestServer) CloseConnection(_ context.Context, request *wirev1.CloseConnectionRequest) (*wirev1.CloseConnectionResponse, error) {
	s.record("close-connection", request.GetConnectionId().GetValue(), "")

	return &wirev1.CloseConnectionResponse{}, nil
}

func (s *clientTestServer) Compile(_ context.Context, request *wirev1.CompileRequest) (*wirev1.CompileResponse, error) {
	plan, err := s.compile(request.GetConnectionId(), request.GetSource(), false)
	if err != nil {
		return nil, err
	}

	return &wirev1.CompileResponse{Plan: plan}, nil
}

func (s *clientTestServer) CompileDebug(_ context.Context, request *wirev1.CompileDebugRequest) (*wirev1.CompileDebugResponse, error) {
	plan, err := s.compile(request.GetConnectionId(), request.GetSource(), true)
	if err != nil {
		return nil, err
	}

	return &wirev1.CompileDebugResponse{Plan: plan}, nil
}

func (s *clientTestServer) compile(connectionID *wirev1.ConnectionId, source *wirev1.Source, debug bool) (*wirev1.Plan, error) {
	s.mu.Lock()
	s.calls = append(s.calls, call("compile", connectionID.GetValue(), ""))
	s.lastCompileDebuggable = debug
	s.lastCompileSourceName = source.GetName()
	s.lastCompileContent = source.GetContent()

	err := s.compileErr
	if err == nil {
		s.plans++
	}

	planID := fmt.Sprintf("plan-%d", s.plans)
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &wirev1.Plan{Id: &wirev1.PlanId{Value: planID}}, nil
}

func (s *clientTestServer) ReleasePlan(ctx context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
	s.mu.Lock()
	s.releasePlanCalls++
	s.calls = append(s.calls, call("release-plan", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue()))
	s.releasePlanLimit, _ = ctx.Deadline()
	err := s.releasePlanErr
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &wirev1.ReleasePlanResponse{}, nil
}

func (s *clientTestServer) Run(_ context.Context, request *wirev1.RunRequest) (*wirev1.RunResponse, error) {
	value, err := s.startExecution("run", request.GetConnectionId().GetValue(), "", request.GetOutputContentType())

	return &wirev1.RunResponse{Execution: value}, err
}

func (s *clientTestServer) RunSession(_ context.Context, request *wirev1.RunSessionRequest) (*wirev1.RunSessionResponse, error) {
	value, err := s.startExecution("run-session", request.GetConnectionId().GetValue(), request.GetSessionId().GetValue(), "")

	return &wirev1.RunSessionResponse{Execution: value}, err
}

func (s *clientTestServer) startExecution(operation, connectionID, parentID, contentType string) (*wirev1.Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, call(operation, connectionID, parentID))

	s.lastOutputContentType = contentType
	if s.executeErr != nil {
		return nil, s.executeErr
	}

	s.executions++

	return executionSnapshotProto(fmt.Sprintf("execution-%d", s.executions), wirev1.ExecutionState_EXECUTION_STATE_RUNNING, nil, nil), nil
}

func (s *clientTestServer) CreateSession(_ context.Context, request *wirev1.CreateSessionRequest) (*wirev1.CreateSessionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sessions++
	s.calls = append(s.calls, call("new-session", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue()))
	id := fmt.Sprintf("session-%d", s.sessions)

	return &wirev1.CreateSessionResponse{Session: &wirev1.Session{Id: &wirev1.SessionId{Value: id}}}, nil
}

func (s *clientTestServer) ReleaseSession(_ context.Context, request *wirev1.ReleaseSessionRequest) (*wirev1.ReleaseSessionResponse, error) {
	s.record("release-session", request.GetConnectionId().GetValue(), request.GetSessionId().GetValue())

	return &wirev1.ReleaseSessionResponse{}, nil
}

func (s *clientTestServer) ReleaseExecution(ctx context.Context, request *wirev1.ReleaseExecutionRequest) (*wirev1.ReleaseExecutionResponse, error) {
	s.mu.Lock()
	s.releaseExecutionCalls++
	s.calls = append(s.calls, call("release-execution", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue()))
	s.releaseExecutionLimit, _ = ctx.Deadline()
	err := s.releaseExecutionErr
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &wirev1.ReleaseExecutionResponse{}, nil
}

func (s *clientTestServer) WatchExecution(request *wirev1.WatchExecutionRequest, stream wirev1.ExecutionService_WatchExecutionServer) error {
	s.mu.Lock()
	s.calls = append(s.calls, call("watch", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue()))
	index := s.watchCalls
	s.watchCalls++
	var script executionWatchScript

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

func (s *clientTestServer) record(name string, connectionID string, resourceID string) {
	s.mu.Lock()
	s.calls = append(s.calls, call(name, connectionID, resourceID))
	s.mu.Unlock()
}

func (s *clientTestServer) callSnapshot() ([]string, int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.calls...), s.watchCalls, s.releaseExecutionCalls, s.releasePlanCalls
}

func (s *clientTestServer) releaseDeadlineSnapshot() (time.Time, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.releaseExecutionLimit, s.releasePlanLimit
}

func assertCleanupDeadline(t *testing.T, kind string, deadline time.Time) {
	t.Helper()

	if deadline.IsZero() {
		t.Fatalf("%s cleanup has no deadline", kind)
	}

	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > convenienceCleanupTimeout {
		t.Fatalf("unexpected %s cleanup deadline: %v", kind, remaining)
	}
}

func TestRuntimeRunOwnsItsExecution(t *testing.T) {
	t.Run("success and options", func(t *testing.T) {
		server := &clientTestServer{watchScripts: []executionWatchScript{{events: []*wirev1.WatchExecutionResponse{
			executionCompletedEvent("execution-1", "application/json", []byte(`{"value":1}`)),
		}}}}
		client := openTestRuntime(t, startClientTestServer(t, server))

		output, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"}, api.WithOutputContentType("application/json"))
		if err != nil || string(output.Content) != `{"value":1}` {
			t.Fatalf("unexpected Runtime.Run result: %#v, %v", output, err)
		}

		calls, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		want := []string{
			call("run", "connection", ""),
			call("watch", "connection", "execution-1"),
			call("release-execution", "connection", "execution-1"),
		}
		server.mu.Lock()
		debuggable := server.lastCompileDebuggable
		contentType := server.lastOutputContentType
		server.mu.Unlock()

		if !slices.Equal(calls, want) || debuggable || contentType != "application/json" || releaseExecutionCalls != 1 || releasePlanCalls != 0 {
			t.Fatalf("Runtime.Run orchestration: calls=%v debug=%v content=%q releases=%d/%d", calls, debuggable, contentType, releaseExecutionCalls, releasePlanCalls)
		}
	})

	t.Run("compile failure creates nothing", func(t *testing.T) {
		server := &clientTestServer{compileErr: status.Error(codes.InvalidArgument, "compile failed")}
		client := openTestRuntime(t, startClientTestServer(t, server))

		_, err := client.Compile(testClientContext(t), api.Source{Content: "invalid"})

		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Message != "compile failed" {
			t.Fatalf("unexpected compile failure: %v", err)
		}

		calls, watchCalls, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if !slices.Equal(calls, []string{call("compile", "connection", "")}) || watchCalls != 0 || releaseExecutionCalls != 0 || releasePlanCalls != 0 {
			t.Fatalf("compile failure leaked cleanup calls: %v", calls)
		}
	})

	t.Run("run rejection creates no resources", func(t *testing.T) {
		server := &clientTestServer{executeErr: status.Error(codes.InvalidArgument, "execute failed")}
		client := openTestRuntime(t, startClientTestServer(t, server))

		_, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"})

		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Message != "execute failed" {
			t.Fatalf("unexpected execute failure: %v", err)
		}

		calls, watchCalls, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()

		want := []string{
			call("run", "connection", ""),
		}
		if !slices.Equal(calls, want) || watchCalls != 0 || releaseExecutionCalls != 0 || releasePlanCalls != 0 {
			t.Fatalf("execute failure cleanup: %v", calls)
		}
	})

	t.Run("caller cancellation still cleans up", func(t *testing.T) {
		entered := make(chan struct{})
		server := &clientTestServer{watchScripts: []executionWatchScript{{
			events:  []*wirev1.WatchExecutionResponse{executionStartedEvent("execution-1")},
			block:   true,
			entered: entered,
		}}}
		client := openTestRuntime(t, startClientTestServer(t, server))
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := client.Run(ctx, api.Source{Content: "RETURN 1"})
			result <- err
		}()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("Runtime.Run did not begin waiting")
		}

		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Runtime.Run lost caller cancellation: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Runtime.Run cleanup did not settle")
		}

		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 0 {
			t.Fatalf("cancelled Runtime.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}

		executionDeadline, planDeadline := server.releaseDeadlineSnapshot()
		assertCleanupDeadline(t, "execution", executionDeadline)

		if !planDeadline.IsZero() {
			t.Fatal("direct run released a plan")
		}
	})

	t.Run("stream failure still cleans up", func(t *testing.T) {
		server := &clientTestServer{watchScripts: []executionWatchScript{{
			events: []*wirev1.WatchExecutionResponse{executionStartedEvent("execution-1")},
			err:    status.Error(codes.Unavailable, "watch transport failed"),
		}}}
		client := openTestRuntime(t, startClientTestServer(t, server))

		_, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"})

		var wireErr *Error
		if !errors.As(err, &wireErr) || status.Code(err) != codes.Unavailable || wireErr.Message != "watch transport failed" {
			t.Fatalf("Runtime.Run lost the stream failure: %v", err)
		}

		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 0 {
			t.Fatalf("stream-failed Runtime.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
	})

	t.Run("execution and cleanup failures are joined", func(t *testing.T) {
		server := &clientTestServer{
			watchScripts: []executionWatchScript{{events: []*wirev1.WatchExecutionResponse{
				executionFailedEvent("execution-1", []byte("partial"), &wirev1.Failure{
					Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
					Message:  "execution failed",
				}),
			}}},
			releaseExecutionErr: status.Error(codes.Internal, "execution cleanup failed"),
		}
		client := openTestRuntime(t, startClientTestServer(t, server))

		output, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"})

		var terminalFailure *failure.Failure
		if string(output.Content) != "partial" || !errors.As(err, &terminalFailure) || terminalFailure.Message != "execution failed" ||
			!strings.Contains(err.Error(), "execution cleanup failed") {
			t.Fatalf("Runtime.Run did not preserve all errors: %#v, %v", output, err)
		}

		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 0 {
			t.Fatalf("failed Runtime.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
	})
}

func openTestClient(t *testing.T, connection grpc.ClientConnInterface) *connectionHandle {
	t.Helper()

	client, err := newConnection(testClientContext(t), connection)
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

func startClientTestServer(t *testing.T, implementation *clientTestServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	wirev1.RegisterRuntimeServiceServer(server, implementation)
	wirev1.RegisterPlanServiceServer(server, implementation)
	wirev1.RegisterExecutionServiceServer(server, implementation)
	wirev1.RegisterSessionServiceServer(server, implementation)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///client-domain-test",
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

func openTestRuntime(t *testing.T, connection grpc.ClientConnInterface) api.Runtime {
	t.Helper()

	runtime, err := New(testClientContext(t), connection)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("runtime cleanup failed: %v", err)
		}
	})

	return runtime
}
