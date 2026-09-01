package grpcserver

import (
	"context"

	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

type debugCommandRequest interface {
	GetConnectionId() *wirev1.ConnectionId
	GetDebugSessionId() *wirev1.DebugSessionId
}

func (s *Server) CreateDebugSession(
	ctx context.Context,
	request *wirev1.CreateDebugSessionRequest,
) (*wirev1.CreateDebugSessionResponse, error) {
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

	return &wirev1.CreateDebugSessionResponse{Session: converted}, nil
}

func (s *Server) debugCommand(request debugCommandRequest) (*core.Connection, core.DebugSessionID, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, "", err
	}

	return connection, core.DebugSessionID(request.GetDebugSessionId().GetValue()), nil
}

func (s *Server) Start(ctx context.Context, request *wirev1.StartRequest) (*wirev1.StartResponse, error) {
	connection, id, err := s.debugCommand(request)
	if err != nil {
		return nil, err
	}

	if _, err := connection.StartDebug(ctx, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StartResponse{}, nil
}

func (s *Server) Continue(ctx context.Context, request *wirev1.ContinueRequest) (*wirev1.ContinueResponse, error) {
	connection, id, err := s.debugCommand(request)
	if err != nil {
		return nil, err
	}

	if _, err := connection.ContinueDebug(ctx, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ContinueResponse{}, nil
}

func (s *Server) Pause(ctx context.Context, request *wirev1.PauseRequest) (*wirev1.PauseResponse, error) {
	connection, id, err := s.debugCommand(request)
	if err != nil {
		return nil, err
	}

	if _, err := connection.PauseDebug(ctx, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.PauseResponse{}, nil
}

func (s *Server) Next(ctx context.Context, request *wirev1.NextRequest) (*wirev1.NextResponse, error) {
	connection, id, err := s.debugCommand(request)
	if err != nil {
		return nil, err
	}

	if _, err := connection.NextDebug(ctx, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.NextResponse{}, nil
}

func (s *Server) Step(ctx context.Context, request *wirev1.StepRequest) (*wirev1.StepResponse, error) {
	connection, id, err := s.debugCommand(request)
	if err != nil {
		return nil, err
	}

	if _, err := connection.StepDebug(ctx, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepResponse{}, nil
}

func (s *Server) Out(ctx context.Context, request *wirev1.OutRequest) (*wirev1.OutResponse, error) {
	connection, id, err := s.debugCommand(request)
	if err != nil {
		return nil, err
	}

	if _, err := connection.OutDebug(ctx, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.OutResponse{}, nil
}

func (s *Server) Terminate(ctx context.Context, request *wirev1.TerminateRequest) (*wirev1.TerminateResponse, error) {
	connection, id, err := s.debugCommand(request)
	if err != nil {
		return nil, err
	}

	if _, err := connection.StopDebug(ctx, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.TerminateResponse{}, nil
}

func (s *Server) SetBreakpoint(ctx context.Context, request *wirev1.SetBreakpointRequest) (*wirev1.SetBreakpointResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	location, err := sourceLocationFromProto(request.GetLocation(), "breakpoint location")
	if err != nil {
		return nil, rpcError(err)
	}

	options, err := breakpointOptions(request.GetOptions())
	if err != nil {
		return nil, rpcError(err)
	}

	value, err := connection.SetBreakpointAt(
		ctx,
		core.DebugSessionID(request.GetDebugSessionId().GetValue()),
		location,
		options,
	)
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

func (s *Server) ReleaseDebugSession(
	ctx context.Context,
	request *wirev1.ReleaseDebugSessionRequest,
) (*wirev1.ReleaseDebugSessionResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	if err := connection.ReleaseDebugSession(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseDebugSessionResponse{}, nil
}
