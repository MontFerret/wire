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

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
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

		mu                    sync.Mutex
		plans                 int
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
	}
)

func (s *clientTestServer) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	if err := stream.Send(&wirev1.ConnectResponse{Opened: &wirev1.ConnectionOpened{
		ConnectionId: &wirev1.ConnectionId{Value: "connection"},
		RuntimeInfo:  &wirev1.RuntimeInfo{ApiIdentity: "ferret.wire.v1"},
	}}); err != nil {
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
	s.mu.Lock()
	s.calls = append(s.calls, call("compile", request.GetConnectionId().GetValue(), ""))
	s.lastCompileDebuggable = request.GetOptions().GetDebuggable()
	s.lastCompileSourceName = request.GetSource().GetIdentity()
	s.lastCompileContent = request.GetSource().GetContent()
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

func (s *clientTestServer) ReleasePlan(_ context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
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

func (s *clientTestServer) Execute(_ context.Context, request *wirev1.ExecuteRequest) (*wirev1.ExecuteResponse, error) {
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

	return &wirev1.ExecuteResponse{Execution: executionSnapshotProto(executionID, wirev1.ExecutionState_EXECUTION_STATE_RUNNING, nil, nil)}, nil
}

func (s *clientTestServer) ReleaseExecution(_ context.Context, request *wirev1.ReleaseExecutionRequest) (*wirev1.ReleaseExecutionResponse, error) {
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

func TestClientRunOwnsCreatedResources(t *testing.T) {
	t.Run("success and options", func(t *testing.T) {
		server := &clientTestServer{watchScripts: []executionWatchScript{{events: []*wirev1.WatchExecutionResponse{
			executionCompletedEvent("execution-1", "application/json", []byte(`{"value":1}`)),
		}}}}
		client := openTestClient(t, startClientTestServer(t, server))

		output, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"}, nil, RunOptions{
			Compile: CompileOptions{Debuggable: true},
			Execute: ExecuteOptions{OutputContentType: "application/json"},
		})
		if err != nil || string(output.Content) != `{"value":1}` {
			t.Fatalf("unexpected Client.Run result: %#v, %v", output, err)
		}

		calls, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
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
		server := &clientTestServer{compileErr: status.Error(codes.InvalidArgument, "compile failed")}
		client := openTestClient(t, startClientTestServer(t, server))

		_, err := client.Run(testClientContext(t), api.Source{Content: "invalid"}, nil, RunOptions{})
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Message != "compile failed" {
			t.Fatalf("unexpected compile failure: %v", err)
		}
		calls, watchCalls, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if !slices.Equal(calls, []string{call("compile", "connection", "")}) || watchCalls != 0 || releaseExecutionCalls != 0 || releasePlanCalls != 0 {
			t.Fatalf("compile failure leaked cleanup calls: %v", calls)
		}
	})

	t.Run("execute failure releases plan", func(t *testing.T) {
		server := &clientTestServer{executeErr: status.Error(codes.InvalidArgument, "execute failed")}
		client := openTestClient(t, startClientTestServer(t, server))

		_, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"}, nil, RunOptions{})
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Message != "execute failed" {
			t.Fatalf("unexpected execute failure: %v", err)
		}
		calls, watchCalls, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
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
		server := &clientTestServer{watchScripts: []executionWatchScript{{
			events:  []*wirev1.WatchExecutionResponse{executionStartedEvent("execution-1")},
			block:   true,
			entered: entered,
		}}}
		client := openTestClient(t, startClientTestServer(t, server))
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := client.Run(ctx, api.Source{Content: "RETURN 1"}, nil, RunOptions{})
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
		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 1 {
			t.Fatalf("cancelled Client.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
	})

	t.Run("stream failure still cleans up", func(t *testing.T) {
		server := &clientTestServer{watchScripts: []executionWatchScript{{
			events: []*wirev1.WatchExecutionResponse{executionStartedEvent("execution-1")},
			err:    status.Error(codes.Unavailable, "watch transport failed"),
		}}}
		client := openTestClient(t, startClientTestServer(t, server))

		_, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"}, nil, RunOptions{})
		var wireErr *Error
		if !errors.As(err, &wireErr) || status.Code(err) != codes.Unavailable || wireErr.Message != "watch transport failed" {
			t.Fatalf("Client.Run lost the stream failure: %v", err)
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 1 {
			t.Fatalf("stream-failed Client.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
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
			releasePlanErr:      status.Error(codes.Unavailable, "plan cleanup failed"),
		}
		client := openTestClient(t, startClientTestServer(t, server))

		output, err := client.Run(testClientContext(t), api.Source{Content: "RETURN 1"}, nil, RunOptions{})
		var failure *Failure
		if string(output.Content) != "partial" || !errors.As(err, &failure) || failure.Message != "execution failed" ||
			!strings.Contains(err.Error(), "execution cleanup failed") || !strings.Contains(err.Error(), "plan cleanup failed") {
			t.Fatalf("Client.Run did not preserve all errors: %#v, %v", output, err)
		}
		_, _, releaseExecutionCalls, releasePlanCalls := server.callSnapshot()
		if releaseExecutionCalls != 1 || releasePlanCalls != 1 {
			t.Fatalf("failed Client.Run cleanup: execution=%d plan=%d", releaseExecutionCalls, releasePlanCalls)
		}
	})
}

func openTestClient(t *testing.T, connection grpc.ClientConnInterface) *Client {
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

func startClientTestServer(t *testing.T, implementation *clientTestServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	wirev1.RegisterRuntimeServiceServer(server, implementation)
	wirev1.RegisterPlanServiceServer(server, implementation)
	wirev1.RegisterExecutionServiceServer(server, implementation)
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
