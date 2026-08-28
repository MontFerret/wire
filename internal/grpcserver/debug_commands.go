package grpcserver

import (
	"context"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

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

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.OpenDebugSessionResponse{Session: converted}, nil
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

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StartDebugResponse{Session: converted}, nil
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

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ContinueResponse{Session: converted}, nil
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

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.PauseResponse{Session: converted}, nil
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

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.NextResponse{Session: converted}, nil
}

func (s *Server) Step(ctx context.Context, request *wirev1.StepRequest) (*wirev1.StepResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}

	snapshot, err := connection.StepDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepResponse{Session: converted}, nil
}

func (s *Server) Out(ctx context.Context, request *wirev1.OutRequest) (*wirev1.OutResponse, error) {
	connection, id, err := s.debugCommand(request.GetCommand())
	if err != nil {
		return nil, err
	}

	snapshot, err := connection.OutDebug(ctx, id)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.OutResponse{Session: converted}, nil
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

	converted, err := debugSession(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StopDebugResponse{Session: converted}, nil
}

func (s *Server) SetBreakpoint(ctx context.Context, request *wirev1.SetBreakpointRequest) (*wirev1.SetBreakpointResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	requested := request.GetLocation()
	value, err := connection.SetBreakpoint(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), source.Location{
		Position: source.Position{Line: int(requested.GetLine()), Column: int(requested.GetColumn())},
		File:     requested.GetFile(),
	})
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := breakpoint(value)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.SetBreakpointResponse{Breakpoint: converted}, nil
}

func (s *Server) DeleteBreakpoint(ctx context.Context, request *wirev1.DeleteBreakpointRequest) (*wirev1.DeleteBreakpointResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	id, err := debuggerIDFromProto[debugger.BreakpointID](request.GetBreakpointId(), "breakpoint ID")
	if err != nil {
		return nil, rpcError(err)
	}

	if err := connection.DeleteBreakpoint(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.DeleteBreakpointResponse{}, nil
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
