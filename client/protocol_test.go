package client

import (
	"context"
	"errors"
	"io"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type (
	protocolPlanClient struct {
		compileRequest  *wirev1.CompileRequest
		compileResponse *wirev1.CompileResponse
		compileErr      error
		releaseRequest  *wirev1.ReleasePlanRequest
	}

	protocolExecutionClient struct {
		executeRequest  *wirev1.ExecuteRequest
		executeResponse *wirev1.ExecuteResponse
		executeErr      error
		cancelRequest   *wirev1.CancelExecutionRequest
		watchRequest    *wirev1.WatchExecutionRequest
		watchStream     grpc.ServerStreamingClient[wirev1.WatchExecutionResponse]
		watchErr        error
		releaseRequest  *wirev1.ReleaseExecutionRequest
	}

	protocolExecutionStream struct {
		ctx    context.Context
		events []*wirev1.WatchExecutionResponse
		err    error
	}
)

func (c *protocolPlanClient) Compile(_ context.Context, request *wirev1.CompileRequest, _ ...grpc.CallOption) (*wirev1.CompileResponse, error) {
	c.compileRequest = request

	return c.compileResponse, c.compileErr
}

func (c *protocolPlanClient) ReleasePlan(_ context.Context, request *wirev1.ReleasePlanRequest, _ ...grpc.CallOption) (*wirev1.ReleasePlanResponse, error) {
	c.releaseRequest = request

	return &wirev1.ReleasePlanResponse{}, nil
}

func (c *protocolExecutionClient) Execute(_ context.Context, request *wirev1.ExecuteRequest, _ ...grpc.CallOption) (*wirev1.ExecuteResponse, error) {
	c.executeRequest = request

	return c.executeResponse, c.executeErr
}

func (c *protocolExecutionClient) CancelExecution(_ context.Context, request *wirev1.CancelExecutionRequest, _ ...grpc.CallOption) (*wirev1.CancelExecutionResponse, error) {
	c.cancelRequest = request

	return &wirev1.CancelExecutionResponse{}, nil
}

func (c *protocolExecutionClient) WatchExecution(_ context.Context, request *wirev1.WatchExecutionRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[wirev1.WatchExecutionResponse], error) {
	c.watchRequest = request

	return c.watchStream, c.watchErr
}

func (c *protocolExecutionClient) ReleaseExecution(_ context.Context, request *wirev1.ReleaseExecutionRequest, _ ...grpc.CallOption) (*wirev1.ReleaseExecutionResponse, error) {
	c.releaseRequest = request

	return &wirev1.ReleaseExecutionResponse{}, nil
}

func (s *protocolExecutionStream) Recv() (*wirev1.WatchExecutionResponse, error) {
	if len(s.events) > 0 {
		event := s.events[0]
		s.events = s.events[1:]

		return event, nil
	}

	if s.err != nil {
		err := s.err
		s.err = nil

		return nil, err
	}

	return nil, io.EOF
}

func (s *protocolExecutionStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *protocolExecutionStream) Trailer() metadata.MD {
	return nil
}

func (s *protocolExecutionStream) CloseSend() error {
	return nil
}

func (s *protocolExecutionStream) Context() context.Context {
	return s.ctx
}

func (s *protocolExecutionStream) SendMsg(any) error {
	return nil
}

func (s *protocolExecutionStream) RecvMsg(any) error {
	return nil
}

func TestProtocolPlanBuildsRequestsAndReturnsDomainMetadata(t *testing.T) {
	implementation := &protocolPlanClient{compileResponse: &wirev1.CompileResponse{Plan: &wirev1.Plan{
		Id:         &wirev1.PlanId{Value: "plan"},
		Parameters: []string{"input"},
		Debuggable: true,
	}}}
	protocol := &protocolClient{connectionID: "connection", planClient: implementation}

	compiled, err := protocol.compile(context.Background(), Source{Content: "RETURN @input", Identity: "query.fql"}, CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}

	request := implementation.compileRequest
	if request.GetConnectionId().GetValue() != "connection" || request.GetSource().GetContent() != "RETURN @input" ||
		request.GetSource().GetIdentity() != "query.fql" || !request.GetOptions().GetDebuggable() {
		t.Fatalf("unexpected compile request: %#v", request)
	}

	if compiled.id != "plan" || len(compiled.parameters) != 1 || compiled.parameters[0] != "input" || !compiled.debuggable {
		t.Fatalf("unexpected compiled plan: %#v", compiled)
	}

	implementation.compileResponse.Plan.Parameters[0] = "changed"
	if compiled.parameters[0] != "input" {
		t.Fatalf("compiled metadata retained protobuf storage: %v", compiled.parameters)
	}

	if err := protocol.releasePlan(context.Background(), "plan"); err != nil {
		t.Fatal(err)
	}

	if implementation.releaseRequest.GetConnectionId().GetValue() != "connection" || implementation.releaseRequest.GetPlanId().GetValue() != "plan" {
		t.Fatalf("unexpected release request: %#v", implementation.releaseRequest)
	}
}

func TestProtocolPlanRejectsInvalidAndRemoteResponses(t *testing.T) {
	tests := []struct {
		name     string
		response *wirev1.CompileResponse
		err      error
		message  string
	}{
		{name: "missing plan", response: &wirev1.CompileResponse{}, message: "Wire server returned an invalid compiled plan"},
		{name: "missing plan ID", response: &wirev1.CompileResponse{Plan: &wirev1.Plan{}}, message: "Wire server returned an invalid compiled plan"},
		{name: "remote failure", err: status.Error(codes.Unavailable, "compile unavailable"), message: "compile unavailable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			implementation := &protocolPlanClient{compileResponse: test.response, compileErr: test.err}
			protocol := &protocolClient{connectionID: "connection", planClient: implementation}

			_, err := protocol.compile(context.Background(), Source{}, CompileOptions{})
			if err == nil || err.Error() != test.message {
				t.Fatalf("compile error = %v", err)
			}
		})
	}
}

func TestProtocolExecutionBuildsRequestsAndConvertsEvents(t *testing.T) {
	failed := executionFailedEvent("execution", []byte("partial"), &wirev1.Failure{
		Category: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE,
		Message:  "failed",
	})
	stream := &protocolExecutionStream{ctx: context.Background(), events: []*wirev1.WatchExecutionResponse{
		executionStartedEvent("execution"),
		executionCompletedEvent("execution", "application/json", []byte("complete")),
		failed,
		executionCancelledEvent("execution"),
	}}
	implementation := &protocolExecutionClient{
		executeResponse: &wirev1.ExecuteResponse{Execution: executionSnapshotProto("execution", wirev1.ExecutionState_EXECUTION_STATE_RUNNING, nil, nil)},
		watchStream:     stream,
	}
	protocol := &protocolClient{connectionID: "connection", executionClient: implementation}

	id, err := protocol.execute(context.Background(), "plan", Parameters{"input": int64(1)}, ExecuteOptions{OutputContentType: "application/json"})
	if err != nil || id != "execution" {
		t.Fatalf("execute result = %q, %v", id, err)
	}

	request := implementation.executeRequest
	if request.GetConnectionId().GetValue() != "connection" || request.GetPlanId().GetValue() != "plan" ||
		request.GetOutputContentType() != "application/json" || request.GetParameters().GetValues()["input"].GetIntegerValue() != 1 {
		t.Fatalf("unexpected execute request: %#v", request)
	}

	if err := protocol.cancelExecution(context.Background(), "execution"); err != nil {
		t.Fatal(err)
	}

	events, err := protocol.watchExecution(context.Background(), "execution")
	if err != nil {
		t.Fatal(err)
	}

	if err := protocol.releaseExecution(context.Background(), "execution"); err != nil {
		t.Fatal(err)
	}

	for i, want := range []ExecutionState{ExecutionRunning, ExecutionCompleted, ExecutionFailed, ExecutionCancelled} {
		event, err := events.recv()
		if err != nil || event.Snapshot.State != want {
			t.Fatalf("event %d = %#v, %v", i, event, err)
		}

		if want == ExecutionFailed {
			failed.GetFailed().GetExecution().GetOutput().Content[0] = 'X'
			if string(event.Snapshot.Output.Content) != "partial" || event.Snapshot.Failure.Message != "failed" {
				t.Fatalf("failed event retained protobuf storage: %#v", event)
			}
		}
	}

	if implementation.cancelRequest.GetConnectionId().GetValue() != "connection" || implementation.cancelRequest.GetExecutionId().GetValue() != "execution" ||
		implementation.watchRequest.GetConnectionId().GetValue() != "connection" || implementation.watchRequest.GetExecutionId().GetValue() != "execution" ||
		implementation.releaseRequest.GetConnectionId().GetValue() != "connection" || implementation.releaseRequest.GetExecutionId().GetValue() != "execution" {
		t.Fatalf("execution IDs were not propagated: cancel=%#v watch=%#v release=%#v", implementation.cancelRequest, implementation.watchRequest, implementation.releaseRequest)
	}
}

func TestProtocolExecutionRejectsInvalidResponsesAndStreamFailures(t *testing.T) {
	tests := []struct {
		name     string
		response *wirev1.ExecuteResponse
		err      error
		message  string
	}{
		{name: "missing execution", response: &wirev1.ExecuteResponse{}, message: "Wire server returned an invalid execution"},
		{name: "missing execution ID", response: &wirev1.ExecuteResponse{Execution: &wirev1.Execution{}}, message: "Wire server returned an invalid execution"},
		{name: "remote failure", err: status.Error(codes.Unavailable, "execute unavailable"), message: "execute unavailable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			implementation := &protocolExecutionClient{executeResponse: test.response, executeErr: test.err}
			protocol := &protocolClient{connectionID: "connection", executionClient: implementation}

			_, err := protocol.execute(context.Background(), "plan", nil, ExecuteOptions{})
			if err == nil || err.Error() != test.message {
				t.Fatalf("execute error = %v", err)
			}
		})
	}

	t.Run("missing stream", func(t *testing.T) {
		implementation := &protocolExecutionClient{}
		protocol := &protocolClient{connectionID: "connection", executionClient: implementation}

		_, err := protocol.watchExecution(context.Background(), "execution")
		if err == nil || err.Error() != "Wire server returned no execution event stream" {
			t.Fatalf("missing stream error = %v", err)
		}
	})

	t.Run("empty event", func(t *testing.T) {
		events := &protocolExecutionEvents{stream: &protocolExecutionStream{
			ctx:    context.Background(),
			events: []*wirev1.WatchExecutionResponse{{}},
		}}

		_, err := events.recv()
		if err == nil || err.Error() != "Wire server returned an empty execution event" {
			t.Fatalf("empty event error = %v", err)
		}
	})

	t.Run("stream failure", func(t *testing.T) {
		events := &protocolExecutionEvents{stream: &protocolExecutionStream{
			ctx: context.Background(),
			err: status.Error(codes.Unavailable, "watch unavailable"),
		}}

		_, err := events.recv()
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Message != "watch unavailable" || status.Code(err) != codes.Unavailable {
			t.Fatalf("stream error = %#v", err)
		}
	})
}

func TestExecutionEventsReleaseLocalStreamAfterTerminalOrFailure(t *testing.T) {
	tests := []struct {
		name   string
		stream *protocolExecutionStream
	}{
		{
			name: "terminal event",
			stream: &protocolExecutionStream{
				ctx:    context.Background(),
				events: []*wirev1.WatchExecutionResponse{executionCancelledEvent("execution")},
			},
		},
		{
			name: "stream failure",
			stream: &protocolExecutionStream{
				ctx: context.Background(),
				err: status.Error(codes.Unavailable, "watch unavailable"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cancelled := make(chan struct{})
			events := &ExecutionEvents{
				stream: &protocolExecutionEvents{stream: test.stream},
				cancel: func() { close(cancelled) },
			}

			_, _ = events.Recv()
			select {
			case <-cancelled:
			default:
				t.Fatal("execution events retained its local stream")
			}
		})
	}
}
