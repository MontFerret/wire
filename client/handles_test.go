package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
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

type handleServer struct {
	wirev1.UnimplementedRuntimeServiceServer
	wirev1.UnimplementedPlanServiceServer
	wirev1.UnimplementedExecutionServiceServer
	wirev1.UnimplementedDebugServiceServer

	mu                      sync.Mutex
	connections             int
	executions              int
	calls                   []string
	releaseExecutionCalls   int
	releaseExecutionEntered chan struct{}
	allowExecutionRelease   chan struct{}
	releaseExecutionOnce    sync.Once
	releaseExecutionErr     error
}

func (s *handleServer) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	s.mu.Lock()
	s.connections++
	id := fmt.Sprintf("connection-%d", s.connections)
	s.mu.Unlock()

	if err := stream.Send(&wirev1.ConnectResponse{Opened: &wirev1.ConnectionOpened{
		ConnectionId: &wirev1.ConnectionId{Value: id},
		RuntimeInfo:  &wirev1.RuntimeInfo{ApiIdentity: "ferret.wire.v1"},
	}}); err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *handleServer) CloseConnection(_ context.Context, request *wirev1.CloseConnectionRequest) (*wirev1.CloseConnectionResponse, error) {
	s.record("close-connection", request.GetConnectionId().GetValue(), "")

	return &wirev1.CloseConnectionResponse{}, nil
}

func (s *handleServer) Compile(_ context.Context, request *wirev1.CompileRequest) (*wirev1.CompileResponse, error) {
	connectionID := request.GetConnectionId().GetValue()
	s.record("compile", connectionID, "")

	return &wirev1.CompileResponse{Plan: &wirev1.Plan{
		Id:         &wirev1.PlanId{Value: "plan-" + connectionID},
		Parameters: []string{"input"},
		Debuggable: request.GetOptions().GetDebuggable(),
	}}, nil
}

func (s *handleServer) ReleasePlan(_ context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
	s.record("release-plan", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue())

	return &wirev1.ReleasePlanResponse{}, nil
}

func (s *handleServer) Execute(_ context.Context, request *wirev1.ExecuteRequest) (*wirev1.ExecuteResponse, error) {
	s.mu.Lock()
	s.executions++
	id := fmt.Sprintf("execution-%d", s.executions)
	s.calls = append(s.calls, call("execute", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue()))
	s.mu.Unlock()

	return &wirev1.ExecuteResponse{Execution: executionProto(id, request.GetPlanId().GetValue())}, nil
}

func (s *handleServer) CancelExecution(_ context.Context, request *wirev1.CancelExecutionRequest) (*wirev1.CancelExecutionResponse, error) {
	s.record("cancel", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue())

	return &wirev1.CancelExecutionResponse{Execution: executionProto(request.GetExecutionId().GetValue(), "plan")}, nil
}

func (s *handleServer) ReleaseExecution(_ context.Context, request *wirev1.ReleaseExecutionRequest) (*wirev1.ReleaseExecutionResponse, error) {
	s.mu.Lock()
	s.releaseExecutionCalls++
	s.calls = append(s.calls, call("release-execution", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue()))
	entered := s.releaseExecutionEntered
	allow := s.allowExecutionRelease
	err := s.releaseExecutionErr
	s.mu.Unlock()

	if entered != nil {
		s.releaseExecutionOnce.Do(func() { close(entered) })
	}

	if allow != nil {
		<-allow
	}

	if err != nil {
		return nil, err
	}

	return &wirev1.ReleaseExecutionResponse{}, nil
}

func (s *handleServer) WatchExecution(request *wirev1.WatchExecutionRequest, stream wirev1.ExecutionService_WatchExecutionServer) error {
	s.record("watch-execution", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue())
	id := request.GetExecutionId().GetValue()

	return stream.Send(&wirev1.WatchExecutionResponse{
		ExecutionId: &wirev1.ExecutionId{Value: id},
		Sequence:    1,
		Payload: &wirev1.WatchExecutionResponse_Started{Started: &wirev1.ExecutionStarted{
			Execution: executionProto(id, "plan"),
		}},
	})
}

func (s *handleServer) OpenDebugSession(_ context.Context, request *wirev1.OpenDebugSessionRequest) (*wirev1.OpenDebugSessionResponse, error) {
	connectionID := request.GetConnectionId().GetValue()
	s.record("new-debug", connectionID, request.GetPlanId().GetValue())

	return &wirev1.OpenDebugSessionResponse{Session: debugProto("debug-" + connectionID)}, nil
}

func (s *handleServer) StartDebug(_ context.Context, request *wirev1.StartDebugRequest) (*wirev1.StartDebugResponse, error) {
	s.recordCommand("start", request.GetCommand())

	return &wirev1.StartDebugResponse{Session: debugProto(request.GetCommand().GetDebugSessionId().GetValue())}, nil
}

func (s *handleServer) Continue(_ context.Context, request *wirev1.ContinueRequest) (*wirev1.ContinueResponse, error) {
	s.recordCommand("continue", request.GetCommand())

	return &wirev1.ContinueResponse{Session: debugProto(request.GetCommand().GetDebugSessionId().GetValue())}, nil
}

func (s *handleServer) Pause(_ context.Context, request *wirev1.PauseRequest) (*wirev1.PauseResponse, error) {
	s.recordCommand("pause", request.GetCommand())

	return &wirev1.PauseResponse{Session: debugProto(request.GetCommand().GetDebugSessionId().GetValue())}, nil
}

func (s *handleServer) Next(_ context.Context, request *wirev1.NextRequest) (*wirev1.NextResponse, error) {
	s.recordCommand("next", request.GetCommand())

	return &wirev1.NextResponse{Session: debugProto(request.GetCommand().GetDebugSessionId().GetValue())}, nil
}

func (s *handleServer) Step(_ context.Context, request *wirev1.StepRequest) (*wirev1.StepResponse, error) {
	s.recordCommand("step", request.GetCommand())

	return &wirev1.StepResponse{Session: debugProto(request.GetCommand().GetDebugSessionId().GetValue())}, nil
}

func (s *handleServer) Out(_ context.Context, request *wirev1.OutRequest) (*wirev1.OutResponse, error) {
	s.recordCommand("out", request.GetCommand())

	return &wirev1.OutResponse{Session: debugProto(request.GetCommand().GetDebugSessionId().GetValue())}, nil
}

func (s *handleServer) StopDebug(_ context.Context, request *wirev1.StopDebugRequest) (*wirev1.StopDebugResponse, error) {
	s.recordCommand("stop", request.GetCommand())

	return &wirev1.StopDebugResponse{Session: debugProto(request.GetCommand().GetDebugSessionId().GetValue())}, nil
}

func (s *handleServer) SetBreakpoint(_ context.Context, request *wirev1.SetBreakpointRequest) (*wirev1.SetBreakpointResponse, error) {
	s.record("set-breakpoint", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.SetBreakpointResponse{Breakpoint: &wirev1.Breakpoint{
		Id: 1, File: request.GetLocation().GetFile(), RequestedLine: request.GetLocation().GetLine(),
		Line: request.GetLocation().GetLine(), Verified: true,
	}}, nil
}

func (s *handleServer) DeleteBreakpoint(_ context.Context, request *wirev1.DeleteBreakpointRequest) (*wirev1.DeleteBreakpointResponse, error) {
	s.record("delete-breakpoint", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.DeleteBreakpointResponse{}, nil
}

func (s *handleServer) Frames(_ context.Context, request *wirev1.FramesRequest) (*wirev1.FramesResponse, error) {
	s.record("frames", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.FramesResponse{Frames: []*wirev1.Frame{{Index: 0, Name: "main"}}}, nil
}

func (s *handleServer) FrameLocals(_ context.Context, request *wirev1.FrameLocalsRequest) (*wirev1.FrameLocalsResponse, error) {
	s.record("frame-locals", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.FrameLocalsResponse{Variables: []*wirev1.Variable{{Name: "value", Value: &wirev1.DebugValue{Display: "1"}}}}, nil
}

func (s *handleServer) Variables(_ context.Context, request *wirev1.VariablesRequest) (*wirev1.VariablesResponse, error) {
	s.record("variables", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.VariablesResponse{Variables: []*wirev1.Variable{{Name: "nested", Value: &wirev1.DebugValue{Display: "2"}}}}, nil
}

func (s *handleServer) EvaluateFrame(_ context.Context, request *wirev1.EvaluateFrameRequest) (*wirev1.EvaluateFrameResponse, error) {
	s.record("evaluate", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.EvaluateFrameResponse{Value: &wirev1.DebugValue{Display: "3"}}, nil
}

func (s *handleServer) ReleaseDebugSession(_ context.Context, request *wirev1.ReleaseDebugSessionRequest) (*wirev1.ReleaseDebugSessionResponse, error) {
	s.record("release-debug", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.ReleaseDebugSessionResponse{}, nil
}

func (s *handleServer) WatchDebug(request *wirev1.WatchDebugRequest, stream wirev1.DebugService_WatchDebugServer) error {
	s.record("watch-debug", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())
	id := request.GetDebugSessionId().GetValue()

	return stream.Send(&wirev1.WatchDebugResponse{
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
		Sequence:       1,
		Payload: &wirev1.WatchDebugResponse_Stopped{Stopped: &wirev1.DebugStopped{
			Session: debugProto(id),
		}},
	})
}

func (s *handleServer) recordCommand(name string, command *wirev1.DebugCommand) {
	s.record(name, command.GetConnectionId().GetValue(), command.GetDebugSessionId().GetValue())
}

func (s *handleServer) record(name string, connectionID string, resourceID string) {
	s.mu.Lock()
	s.calls = append(s.calls, call(name, connectionID, resourceID))
	s.mu.Unlock()
}

func (s *handleServer) recordedCalls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.calls...)
}

func call(name string, connectionID string, resourceID string) string {
	return name + "|" + connectionID + "|" + resourceID
}

func executionProto(id string, planID string) *wirev1.Execution {
	return &wirev1.Execution{
		Id: &wirev1.ExecutionId{Value: id}, PlanId: &wirev1.PlanId{Value: planID},
		State: wirev1.ExecutionState_EXECUTION_STATE_RUNNING,
	}
}

func debugProto(id string) *wirev1.DebugSession {
	return &wirev1.DebugSession{
		Id: &wirev1.DebugSessionId{Value: id}, PlanId: &wirev1.PlanId{Value: "plan"},
		State: wirev1.DebugState_DEBUG_STATE_STOPPED,
	}
}

func TestHandlesBindOwnerAndResourceIdentifiers(t *testing.T) {
	implementation := &handleServer{}
	connection := startHandleServer(t, implementation)
	first := openHandleClient(t, connection)
	second := openHandleClient(t, connection)

	plan, err := first.Compile(testClientContext(t), Source{Content: "RETURN @input"}, CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	parameters := plan.Parameters()
	parameters[0] = "changed"
	if got := plan.Parameters(); !slices.Equal(got, []string{"input"}) || !plan.Debuggable() {
		t.Fatalf("plan metadata was not immutable: %v, %v", got, plan.Debuggable())
	}

	if err := second.Close(testClientContext(t)); err != nil {
		t.Fatal(err)
	}

	firstExecution, err := plan.Execute(testClientContext(t), Parameters{"input": 1}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secondExecution, err := plan.Execute(testClientContext(t), Parameters{"input": 2}, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := firstExecution.Cancel(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	executionEvents, err := firstExecution.Watch(testClientContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if event, err := executionEvents.Recv(); err != nil || event.Snapshot.State != ExecutionRunning {
		t.Fatalf("unexpected execution event: %#v, %v", event, err)
	}

	debug, err := plan.NewDebugSession(testClientContext(t), nil, DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for name, command := range map[string]func(context.Context) error{
		"start": debug.Start, "continue": debug.Continue, "pause": debug.Pause,
		"next": debug.Next, "step": debug.Step, "out": debug.Out, "stop": debug.Stop,
	} {
		if err := command(testClientContext(t)); err != nil {
			t.Fatalf("%s failed: %v", name, err)
		}
	}

	breakpoint, err := debug.SetBreakpoint(testClientContext(t), Location{File: "query.fql", Line: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := debug.DeleteBreakpoint(testClientContext(t), breakpoint.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := debug.Frames(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := debug.FrameLocals(testClientContext(t), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := debug.Variables(testClientContext(t), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := debug.EvaluateFrame(testClientContext(t), 0, "1 + 2"); err != nil {
		t.Fatal(err)
	}
	debugEvents, err := debug.Watch(testClientContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if event, err := debugEvents.Recv(); err != nil || event.Snapshot.State != DebugStopped {
		t.Fatalf("unexpected debug event: %#v, %v", event, err)
	}

	if err := debug.Close(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := debug.Close(testClientContext(t)); err != nil {
		t.Fatalf("repeated debug close changed its result: %v", err)
	}
	if err := firstExecution.Close(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := firstExecution.Close(testClientContext(t)); err != nil {
		t.Fatalf("repeated execution close changed its result: %v", err)
	}
	if err := secondExecution.Close(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := plan.Close(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := plan.Close(testClientContext(t)); err != nil {
		t.Fatalf("repeated plan close changed its result: %v", err)
	}

	want := []string{
		call("compile", "connection-1", ""),
		call("execute", "connection-1", "plan-connection-1"),
		call("execute", "connection-1", "plan-connection-1"),
		call("cancel", "connection-1", "execution-1"),
		call("watch-execution", "connection-1", "execution-1"),
		call("new-debug", "connection-1", "plan-connection-1"),
	}
	debugID := "debug-connection-1"
	for _, name := range []string{"start", "continue", "pause", "next", "step", "out", "stop", "set-breakpoint", "delete-breakpoint", "frames", "frame-locals", "variables", "evaluate", "watch-debug", "release-debug"} {
		want = append(want, call(name, "connection-1", debugID))
	}
	want = append(want,
		call("release-execution", "connection-1", "execution-1"),
		call("release-execution", "connection-1", "execution-2"),
		call("release-plan", "connection-1", "plan-connection-1"),
	)
	calls := implementation.recordedCalls()
	for _, expected := range want {
		if !slices.Contains(calls, expected) {
			t.Errorf("missing call %q in %v", expected, calls)
		}
	}
	for _, released := range []string{
		call("release-debug", "connection-1", debugID),
		call("release-execution", "connection-1", "execution-1"),
		call("release-execution", "connection-1", "execution-2"),
		call("release-plan", "connection-1", "plan-connection-1"),
	} {
		if count := countCall(calls, released); count != 1 {
			t.Errorf("release call %q occurred %d times", released, count)
		}
	}
}

func TestHandleCloseRetainsFailureAndRejectsUse(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	implementation := &handleServer{
		releaseExecutionEntered: entered,
		allowExecutionRelease:   allow,
		releaseExecutionErr:     status.Error(codes.Internal, "retained release failure"),
	}
	connection := startHandleServer(t, implementation)
	client := openHandleClient(t, connection)
	plan, err := client.Compile(testClientContext(t), Source{Content: "RETURN 1"}, CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := plan.Execute(testClientContext(t), nil, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	firstCtx := testClientContext(t)
	go func() { first <- execution.Close(firstCtx) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("execution close did not reach the server")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := execution.Close(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter changed retained cleanup: %v", err)
	}
	if err := execution.Cancel(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed execution accepted cancellation: %v", err)
	}

	second := make(chan error, 1)
	secondCtx := testClientContext(t)
	go func() { second <- execution.Close(secondCtx) }()
	close(allow)
	want := <-first
	var wireErr *Error
	if !errors.As(want, &wireErr) || wireErr.Code != codes.Internal || wireErr.Message != "retained release failure" {
		t.Fatalf("unexpected release result: %#v", want)
	}
	if err := execution.Close(testClientContext(t)); !errors.As(err, &wireErr) || err.Error() != want.Error() {
		t.Fatalf("repeated close did not retain the first result: %#v", err)
	}
	if err := <-second; !errors.As(err, &wireErr) || err.Error() != want.Error() {
		t.Fatalf("concurrent close did not retain the first result: %#v", err)
	}

	implementation.mu.Lock()
	releaseCalls := implementation.releaseExecutionCalls
	implementation.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("close issued %d release RPCs", releaseCalls)
	}
}

func TestAncestorCloseInvalidatesDescendants(t *testing.T) {
	implementation := &handleServer{}
	connection := startHandleServer(t, implementation)
	client := openHandleClient(t, connection)
	plan, err := client.Compile(testClientContext(t), Source{Content: "RETURN 1"}, CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := plan.Execute(testClientContext(t), nil, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	debug, err := plan.NewDebugSession(testClientContext(t), nil, DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := plan.Close(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := execution.Cancel(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("execution survived plan close: %v", err)
	}
	if err := debug.Start(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("debug session survived plan close: %v", err)
	}
	if err := execution.Close(testClientContext(t)); err != nil {
		t.Fatalf("execution did not observe ancestor close: %v", err)
	}
	if err := debug.Close(testClientContext(t)); err != nil {
		t.Fatalf("debug session did not observe ancestor close: %v", err)
	}

	calls := implementation.recordedCalls()
	if slices.ContainsFunc(calls, func(value string) bool {
		return value == call("release-execution", "connection-1", "execution-1") ||
			value == call("release-debug", "connection-1", "debug-connection-1")
	}) {
		t.Fatalf("descendants duplicated ancestor cleanup: %v", calls)
	}
}

func TestZeroValueHandlesAreClosed(t *testing.T) {
	var plan Plan
	if _, err := plan.Execute(testClientContext(t), nil, ExecuteOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero plan accepted execution: %v", err)
	}
	if err := plan.Close(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero plan close was not closed: %v", err)
	}

	var execution Execution
	if err := execution.Cancel(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero execution accepted cancellation: %v", err)
	}
	if err := execution.Close(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero execution close was not closed: %v", err)
	}

	var debug DebugSession
	if err := debug.Start(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero debug session accepted start: %v", err)
	}
	if err := debug.Close(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero debug close was not closed: %v", err)
	}
}

func openHandleClient(t *testing.T, connection grpc.ClientConnInterface) *Client {
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

func startHandleServer(t *testing.T, implementation *handleServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	wirev1.RegisterRuntimeServiceServer(server, implementation)
	wirev1.RegisterPlanServiceServer(server, implementation)
	wirev1.RegisterExecutionServiceServer(server, implementation)
	wirev1.RegisterDebugServiceServer(server, implementation)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///client-handles-test",
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

func countCall(calls []string, target string) int {
	count := 0
	for _, value := range calls {
		if value == target {
			count++
		}
	}

	return count
}
