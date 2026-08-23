package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
	"google.golang.org/grpc"
)

type Server struct {
	wirev1.UnimplementedRuntimeServiceServer
	wirev1.UnimplementedPlanServiceServer
	wirev1.UnimplementedExecutionServiceServer
	wirev1.UnimplementedDebugServiceServer

	runtime *core.Runtime
}

func New(runtime *core.Runtime) *Server {
	return &Server{runtime: runtime}
}

func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	wirev1.RegisterRuntimeServiceServer(registrar, s)
	wirev1.RegisterPlanServiceServer(registrar, s)
	wirev1.RegisterExecutionServiceServer(registrar, s)
	wirev1.RegisterDebugServiceServer(registrar, s)
}

func (s *Server) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	connection, err := s.runtime.OpenConnection()
	if err != nil {
		return rpcError(err)
	}
	defer func() {
		_ = s.runtime.CloseConnection(context.Background(), connection.ID())
	}()

	response := &wirev1.ConnectResponse{Payload: &wirev1.ConnectResponse_Opened{Opened: &wirev1.ConnectionOpened{
		ConnectionId: &wirev1.ConnectionId{Value: string(connection.ID())},
		RuntimeInfo:  runtimeInfo(s.runtime.Info()),
	}}}
	if err := stream.Send(response); err != nil {
		return err
	}

	select {
	case <-stream.Context().Done():
		return nil
	case <-connection.Context().Done():
		return nil
	}
}

func (s *Server) CloseConnection(ctx context.Context, request *wirev1.CloseConnectionRequest) (*wirev1.CloseConnectionResponse, error) {
	err := s.runtime.CloseConnection(ctx, core.ConnectionID(request.GetConnectionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.CloseConnectionResponse{}, nil
}

func (s *Server) connection(id *wirev1.ConnectionId) (*core.Connection, error) {
	connection, err := s.runtime.Connection(core.ConnectionID(id.GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}
	return connection, nil
}

func (s *Server) Compile(ctx context.Context, request *wirev1.CompileRequest) (*wirev1.CompileResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.Compile(ctx, core.CompileInput{
		Content:    request.GetSource().GetContent(),
		Identity:   request.GetSource().GetIdentity(),
		Debuggable: request.GetOptions().GetDebuggable(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.CompileResponse{Plan: plan(snapshot)}, nil
}

func (s *Server) ReleasePlan(ctx context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	if err := connection.ReleasePlan(ctx, core.PlanID(request.GetPlanId().GetValue())); err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.ReleasePlanResponse{}, nil
}

func (s *Server) Execute(ctx context.Context, request *wirev1.ExecuteRequest) (*wirev1.ExecuteResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	parameters, err := decodeParameters(request.GetParameters())
	if err != nil {
		return nil, rpcError(&core.DomainError{Category: core.ErrorInvalidRequest, Message: err.Error()})
	}
	snapshot, err := connection.Execute(ctx, core.ExecuteInput{
		PlanID:            core.PlanID(request.GetPlanId().GetValue()),
		Parameters:        parameters,
		OutputContentType: request.GetOutputContentType(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.ExecuteResponse{Execution: execution(snapshot)}, nil
}

func (s *Server) CancelExecution(_ context.Context, request *wirev1.CancelExecutionRequest) (*wirev1.CancelExecutionResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.CancelExecution(core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.CancelExecutionResponse{Execution: execution(snapshot)}, nil
}

func (s *Server) ReleaseExecution(ctx context.Context, request *wirev1.ReleaseExecutionRequest) (*wirev1.ReleaseExecutionResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	if err := connection.ReleaseExecution(ctx, core.ExecutionID(request.GetExecutionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.ReleaseExecutionResponse{}, nil
}

func (s *Server) WatchExecution(request *wirev1.WatchExecutionRequest, stream wirev1.ExecutionService_WatchExecutionServer) error {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return err
	}
	subscription, err := connection.WatchExecution(core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}
	defer subscription.Cancel()
	if subscription.Current.Sequence > 0 {
		if err := stream.Send(executionEvent(subscription.Current)); err != nil {
			return err
		}
	}
	eventsChannel := subscription.Events
	errorsChannel := subscription.Errors
	for {
		select {
		case <-stream.Context().Done():
			return rpcError(stream.Context().Err())
		case event, ok := <-eventsChannel:
			if !ok {
				return subscriptionError(errorsChannel)
			}
			if err := stream.Send(executionEvent(event)); err != nil {
				return err
			}
		case watchErr, ok := <-errorsChannel:
			if ok && watchErr != nil {
				return rpcError(watchErr)
			}
			if !ok {
				errorsChannel = nil
			}
		}
	}
}

func subscriptionError(errors <-chan error) error {
	select {
	case err, ok := <-errors:
		if ok && err != nil {
			return rpcError(err)
		}
	default:
	}
	return nil
}

func (s *Server) OpenDebugSession(ctx context.Context, request *wirev1.OpenDebugSessionRequest) (*wirev1.OpenDebugSessionResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	parameters, err := decodeParameters(request.GetParameters())
	if err != nil {
		return nil, rpcError(&core.DomainError{Category: core.ErrorInvalidRequest, Message: err.Error()})
	}
	snapshot, err := connection.OpenDebugSession(ctx, core.OpenDebugInput{
		PlanID:            core.PlanID(request.GetPlanId().GetValue()),
		Parameters:        parameters,
		OutputContentType: request.GetOutputContentType(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.OpenDebugSessionResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) debugCommand(command *wirev1.DebugCommand) (*core.Connection, core.DebugSessionID, error) {
	if command == nil {
		return nil, "", rpcError(&core.DomainError{Category: core.ErrorInvalidRequest, Message: "debug command is required"})
	}
	connection, err := s.connection(command.GetConnectionId())
	if err != nil {
		return nil, "", err
	}
	return connection, core.DebugSessionID(command.GetDebugSessionId().GetValue()), nil
}

func (s *Server) StartDebug(ctx context.Context, request *wirev1.StartDebugRequest) (*wirev1.StartDebugResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.StartDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.StartDebugResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) Continue(ctx context.Context, request *wirev1.ContinueRequest) (*wirev1.ContinueResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.ContinueDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.ContinueResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) Pause(ctx context.Context, request *wirev1.PauseRequest) (*wirev1.PauseResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.PauseDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.PauseResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) Next(ctx context.Context, request *wirev1.NextRequest) (*wirev1.NextResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.NextDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.NextResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) StepIn(ctx context.Context, request *wirev1.StepInRequest) (*wirev1.StepInResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.StepInDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.StepInResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) StepOut(ctx context.Context, request *wirev1.StepOutRequest) (*wirev1.StepOutResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.StepOutDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.StepOutResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) StopDebug(ctx context.Context, request *wirev1.StopDebugRequest) (*wirev1.StopDebugResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}
	snapshot, err := connection.StopDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.StopDebugResponse{Session: debugSession(snapshot)}, nil
}

func (s *Server) SetBreakpoints(ctx context.Context, request *wirev1.SetBreakpointsRequest) (*wirev1.SetBreakpointsResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	locations := make([]core.BreakpointLocation, len(request.GetBreakpoints()))
	for i, value := range request.GetBreakpoints() {
		if value == nil {
			return nil, rpcError(&core.DomainError{Category: core.ErrorInvalidRequest, Message: "breakpoint location is required"})
		}
		locations[i] = core.BreakpointLocation{Line: int(value.GetLine()), Column: int(value.GetColumn())}
	}
	values, err := connection.SetBreakpoints(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), request.GetFile(), locations)
	if err != nil {
		return nil, rpcError(err)
	}
	result := make([]*wirev1.Breakpoint, len(values))
	for i, value := range values {
		result[i] = breakpoint(value)
	}
	return &wirev1.SetBreakpointsResponse{Breakpoints: result}, nil
}

func (s *Server) StackTrace(ctx context.Context, request *wirev1.StackTraceRequest) (*wirev1.StackTraceResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	values, err := connection.StackTrace(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}
	result := make([]*wirev1.StackFrame, len(values))
	for i, value := range values {
		result[i] = &wirev1.StackFrame{Index: int32(value.Index), Name: value.Name, Location: location(value.Location)}
	}
	return &wirev1.StackTraceResponse{Frames: result}, nil
}

func (s *Server) Scopes(ctx context.Context, request *wirev1.ScopesRequest) (*wirev1.ScopesResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	values, err := connection.Scopes(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), int(request.GetFrameIndex()))
	if err != nil {
		return nil, rpcError(err)
	}
	result := make([]*wirev1.Scope, len(values))
	for i, value := range values {
		variables := make([]*wirev1.Variable, len(value.Variables))
		for j, item := range value.Variables {
			variables[j] = variable(item)
		}
		kind := wirev1.ScopeKind_SCOPE_KIND_LOCALS
		if value.Kind == core.ScopeParameters {
			kind = wirev1.ScopeKind_SCOPE_KIND_PARAMETERS
		}
		result[i] = &wirev1.Scope{Kind: kind, Name: value.Name, Variables: variables}
	}
	return &wirev1.ScopesResponse{Scopes: result}, nil
}

func (s *Server) Variables(ctx context.Context, request *wirev1.VariablesRequest) (*wirev1.VariablesResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	values, err := connection.Variables(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), request.GetReference())
	if err != nil {
		return nil, rpcError(err)
	}
	result := make([]*wirev1.Variable, len(values))
	for i, value := range values {
		result[i] = variable(value)
	}
	return &wirev1.VariablesResponse{Variables: result}, nil
}

func (s *Server) Evaluate(ctx context.Context, request *wirev1.EvaluateRequest) (*wirev1.EvaluateResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	value, err := connection.Evaluate(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), int(request.GetFrameIndex()), request.GetExpression())
	if err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.EvaluateResponse{Value: debugValue(value)}, nil
}

func (s *Server) ReleaseDebugSession(ctx context.Context, request *wirev1.ReleaseDebugSessionRequest) (*wirev1.ReleaseDebugSessionResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	if err := connection.ReleaseDebugSession(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}
	return &wirev1.ReleaseDebugSessionResponse{}, nil
}

func (s *Server) WatchDebug(request *wirev1.WatchDebugRequest, stream wirev1.DebugService_WatchDebugServer) error {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return err
	}
	subscription, err := connection.WatchDebug(core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}
	defer subscription.Cancel()
	if subscription.Current.Sequence > 0 {
		if err := stream.Send(debugEvent(subscription.Current)); err != nil {
			return err
		}
	}
	eventsChannel := subscription.Events
	errorsChannel := subscription.Errors
	for {
		select {
		case <-stream.Context().Done():
			return rpcError(stream.Context().Err())
		case event, ok := <-eventsChannel:
			if !ok {
				return subscriptionError(errorsChannel)
			}
			if err := stream.Send(debugEvent(event)); err != nil {
				return err
			}
		case watchErr, ok := <-errorsChannel:
			if ok && watchErr != nil {
				return rpcError(watchErr)
			}
			if !ok {
				errorsChannel = nil
			}
		}
	}
}
