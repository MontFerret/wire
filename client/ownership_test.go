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

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	wireruntime "github.com/MontFerret/wire/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	releasePlanCalls        int
	releasePlanEntered      chan struct{}
	allowPlanRelease        chan struct{}
	releasePlanOnce         sync.Once
	releasePlanErr          error
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

	if err := stream.Send(&wirev1.ConnectResponse{
		ConnectionId: &wirev1.ConnectionId{Value: id},
		Protocol:     &wirev1.ProtocolInfo{Name: "ferret.wire", Version: "v1"},
	}); err != nil {
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
	}}, nil
}

func (s *handleServer) CompileDebug(_ context.Context, request *wirev1.CompileDebugRequest) (*wirev1.CompileDebugResponse, error) {
	connectionID := request.GetConnectionId().GetValue()
	s.record("compile", connectionID, "")

	return &wirev1.CompileDebugResponse{Plan: &wirev1.Plan{
		Id:         &wirev1.PlanId{Value: "plan-" + connectionID},
		Parameters: []string{"input"},
	}}, nil
}

func (s *handleServer) ReleasePlan(_ context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
	s.mu.Lock()
	s.releasePlanCalls++
	s.calls = append(s.calls, call("release-plan", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue()))
	entered := s.releasePlanEntered
	allow := s.allowPlanRelease
	err := s.releasePlanErr
	s.mu.Unlock()

	if entered != nil {
		s.releasePlanOnce.Do(func() { close(entered) })
	}

	if allow != nil {
		<-allow
	}

	if err != nil {
		return nil, err
	}

	return &wirev1.ReleasePlanResponse{}, nil
}

func (s *handleServer) Execute(_ context.Context, request *wirev1.ExecuteRequest) (*wirev1.ExecuteResponse, error) {
	s.mu.Lock()
	s.executions++
	id := fmt.Sprintf("execution-%d", s.executions)
	s.calls = append(s.calls, call("execute", request.GetConnectionId().GetValue(), request.GetPlanId().GetValue()))
	s.mu.Unlock()

	return &wirev1.ExecuteResponse{Execution: executionProto(id)}, nil
}

func (s *handleServer) CancelExecution(_ context.Context, request *wirev1.CancelExecutionRequest) (*wirev1.CancelExecutionResponse, error) {
	s.record("cancel", request.GetConnectionId().GetValue(), request.GetExecutionId().GetValue())

	return &wirev1.CancelExecutionResponse{}, nil
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
		Sequence:  1,
		Execution: executionProto(id),
	})
}

func (s *handleServer) CreateDebugSession(_ context.Context, request *wirev1.CreateDebugSessionRequest) (*wirev1.CreateDebugSessionResponse, error) {
	connectionID := request.GetConnectionId().GetValue()
	s.record("new-debug", connectionID, request.GetPlanId().GetValue())

	return &wirev1.CreateDebugSessionResponse{Session: debugProto("debug-" + connectionID)}, nil
}

func (s *handleServer) Start(_ context.Context, request *wirev1.StartRequest) (*wirev1.StartResponse, error) {
	s.record("start", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.StartResponse{}, nil
}

func (s *handleServer) Continue(_ context.Context, request *wirev1.ContinueRequest) (*wirev1.ContinueResponse, error) {
	s.record("continue", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.ContinueResponse{}, nil
}

func (s *handleServer) Pause(_ context.Context, request *wirev1.PauseRequest) (*wirev1.PauseResponse, error) {
	s.record("pause", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.PauseResponse{}, nil
}

func (s *handleServer) StepOver(_ context.Context, request *wirev1.StepOverRequest) (*wirev1.StepOverResponse, error) {
	s.record("step-over", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.StepOverResponse{}, nil
}

func (s *handleServer) StepIn(_ context.Context, request *wirev1.StepInRequest) (*wirev1.StepInResponse, error) {
	s.record("step-in", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.StepInResponse{}, nil
}

func (s *handleServer) StepOut(_ context.Context, request *wirev1.StepOutRequest) (*wirev1.StepOutResponse, error) {
	s.record("step-out", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.StepOutResponse{}, nil
}

func (s *handleServer) Terminate(_ context.Context, request *wirev1.TerminateRequest) (*wirev1.TerminateResponse, error) {
	s.record("stop", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.TerminateResponse{}, nil
}

func (s *handleServer) SetBreakpoint(_ context.Context, request *wirev1.SetBreakpointRequest) (*wirev1.SetBreakpointResponse, error) {
	s.record("set-breakpoint", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.SetBreakpointResponse{Breakpoint: &wirev1.Breakpoint{
		Id:                1,
		RequestedLocation: request.GetLocation(),
		Location: &wirev1.Range{
			Location: request.GetLocation(),
			Span:     &wirev1.Span{},
		},
		BindingMode: wirev1.BreakpointBindingMode_BREAKPOINT_BINDING_MODE_NEXT_EXECUTABLE_IN_SOURCE,
		Bound:       true,
	}}, nil
}

func (s *handleServer) DeleteBreakpoint(_ context.Context, request *wirev1.DeleteBreakpointRequest) (*wirev1.DeleteBreakpointResponse, error) {
	s.record("delete-breakpoint", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.DeleteBreakpointResponse{}, nil
}

func (s *handleServer) Frames(_ context.Context, request *wirev1.FramesRequest) (*wirev1.FramesResponse, error) {
	s.record("frames", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.FramesResponse{Frames: []*wirev1.Frame{{
		Name: "main", Location: debugTestLocation("query.fql", 1, 0),
	}}}, nil
}

func (s *handleServer) FrameLocals(_ context.Context, request *wirev1.FrameLocalsRequest) (*wirev1.FrameLocalsResponse, error) {
	s.record("frame-locals", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.FrameLocalsResponse{Variables: []*wirev1.Variable{{
		Name: "value", Value: &wirev1.DebugValue{Type: "int", Display: "1", Reference: 2}, Parameter: true,
	}}}, nil
}

func (s *handleServer) Variables(_ context.Context, request *wirev1.VariablesRequest) (*wirev1.VariablesResponse, error) {
	s.record("variables", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.VariablesResponse{Variables: []*wirev1.Variable{{Name: "nested", Value: &wirev1.DebugValue{Type: "int", Display: "2"}}}}, nil
}

func (s *handleServer) EvaluateFrame(_ context.Context, request *wirev1.EvaluateFrameRequest) (*wirev1.EvaluateFrameResponse, error) {
	s.record("evaluate", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.EvaluateFrameResponse{Value: &wirev1.DebugValue{Type: "int", Display: "3", Reference: 3}}, nil
}

func (s *handleServer) ReleaseDebugSession(_ context.Context, request *wirev1.ReleaseDebugSessionRequest) (*wirev1.ReleaseDebugSessionResponse, error) {
	s.record("release-debug", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())

	return &wirev1.ReleaseDebugSessionResponse{}, nil
}

func (s *handleServer) WatchDebug(request *wirev1.WatchDebugRequest, stream wirev1.DebugService_WatchDebugServer) error {
	s.record("watch-debug", request.GetConnectionId().GetValue(), request.GetDebugSessionId().GetValue())
	id := request.GetDebugSessionId().GetValue()

	return stream.Send(&wirev1.WatchDebugResponse{
		Sequence: 1,
		Kind:     wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED,
		Session:  debugProto(id),
	})
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

func executionProto(id string) *wirev1.Execution {
	return &wirev1.Execution{
		Id:    &wirev1.ExecutionId{Value: id},
		State: wirev1.ExecutionState_EXECUTION_STATE_RUNNING,
	}
}

func debugProto(id string) *wirev1.DebugSession {
	return &wirev1.DebugSession{
		Id:    &wirev1.DebugSessionId{Value: id},
		State: wirev1.DebugState_DEBUG_STATE_STOPPED, StopReason: wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT,
		Location: debugTestRange("query.fql", 1, 0, 0, 0), HitBreakpointIds: []uint64{1},
	}
}

func TestHandleOperationsUseBoundOwnerResources(t *testing.T) {
	implementation := &handleServer{}
	connection := startHandleServer(t, implementation)
	first := openHandleClient(t, connection)
	second := openHandleClient(t, connection)

	plan, err := first.Compile(testClientContext(t), api.Source{Content: "RETURN @input"}, CompileOptions{Debuggable: true})
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
	if event, err := executionEvents.Recv(); err != nil || event.Snapshot.State != wireruntime.StateRunning {
		t.Fatalf("unexpected execution event: %#v, %v", event, err)
	}

	debug, err := plan.NewDebugSession(testClientContext(t), nil, DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for name, command := range map[string]func(context.Context) error{
		"start": debug.Start, "continue": debug.Continue, "pause": debug.Pause,
		"step-over": debug.StepOver, "step-in": debug.StepIn, "step-out": debug.StepOut, "stop": debug.Stop,
	} {
		if err := command(testClientContext(t)); err != nil {
			t.Fatalf("%s failed: %v", name, err)
		}
	}

	breakpoint, err := debug.SetBreakpoint(testClientContext(t), source.Location{
		Position:   source.Position{Line: 1},
		SourceName: "query.fql",
	})
	if err != nil {
		t.Fatal(err)
	}
	if breakpoint.ID != 1 || !breakpoint.Bound || breakpoint.RequestedLocation.SourceName != "query.fql" ||
		breakpoint.Location.SourceName != "query.fql" || breakpoint.Location.Span != (source.Span{}) ||
		breakpoint.PointID != 0 || breakpoint.FunctionID != 0 {
		t.Fatalf("unexpected Unified API breakpoint: %#v", breakpoint)
	}
	if err := debug.DeleteBreakpoint(testClientContext(t), breakpoint.ID); err != nil {
		t.Fatal(err)
	}
	frames, err := debug.Frames(testClientContext(t))
	if err != nil || len(frames) != 1 || frames[0].Name != "main" || frames[0].FunctionID != 0 || frames[0].Location.SourceName != "query.fql" {
		t.Fatalf("unexpected Unified API frames: %#v, %v", frames, err)
	}
	locals, err := debug.FrameLocals(testClientContext(t), 0)
	if err != nil || len(locals) != 1 || !locals[0].Param || locals[0].Value.Reference != 2 {
		t.Fatalf("unexpected Unified API locals: %#v, %v", locals, err)
	}
	variables, err := debug.Variables(testClientContext(t), debugger.ValueReference(1))
	if err != nil || len(variables) != 1 || variables[0].Value.Display != "2" {
		t.Fatalf("unexpected Unified API variables: %#v, %v", variables, err)
	}
	evaluated, err := debug.EvaluateFrame(testClientContext(t), 0, "1 + 2")
	if err != nil || evaluated.Reference != 3 || evaluated.Display != "3" {
		t.Fatalf("unexpected Unified API value: %#v, %v", evaluated, err)
	}
	debugEvents, err := debug.Watch(testClientContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if event, err := debugEvents.Recv(); err != nil || event.Snapshot.State != wiredebugger.StateStopped ||
		event.Snapshot.StopReason != debugger.ReasonBreakpoint || event.Snapshot.Location == nil ||
		event.Snapshot.Location.SourceName != "query.fql" || len(event.Snapshot.HitBreakpointIDs) != 1 || event.Snapshot.HitBreakpointIDs[0] != 1 {
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
	for _, name := range []string{"start", "continue", "pause", "step-over", "step-in", "step-out", "stop", "set-breakpoint", "delete-breakpoint", "frames", "frame-locals", "variables", "evaluate", "watch-debug", "release-debug"} {
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
