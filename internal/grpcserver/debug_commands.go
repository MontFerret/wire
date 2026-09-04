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
	operation, cancel, err := s.operationContext(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	parameters, err := decodeParameters(request.GetParameters())
	if err != nil {
		return nil, rpcError(&core.DomainError{Category: core.ErrorInvalidRequest, Message: err.Error()})
	}

	snapshot, err := s.debugger.Create(operation, core.OpenDebugInput{
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

func (s *Server) debugCommand(
	ctx context.Context,
	request debugCommandRequest,
) (*core.Context, context.CancelFunc, *core.DebugSession, error) {
	operation, cancel, err := s.operationContext(ctx, request.GetConnectionId())
	if err != nil {
		return nil, nil, nil, err
	}

	session, err := s.debugger.Session(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		cancel()

		return nil, nil, nil, rpcError(err)
	}

	return operation, cancel, session, nil
}

func (s *Server) Start(ctx context.Context, request *wirev1.StartRequest) (*wirev1.StartResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Start(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StartResponse{}, nil
}

func (s *Server) Continue(ctx context.Context, request *wirev1.ContinueRequest) (*wirev1.ContinueResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Continue(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ContinueResponse{}, nil
}

func (s *Server) Pause(ctx context.Context, request *wirev1.PauseRequest) (*wirev1.PauseResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Pause(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.PauseResponse{}, nil
}

func (s *Server) StepOver(ctx context.Context, request *wirev1.StepOverRequest) (*wirev1.StepOverResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.StepOver(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepOverResponse{}, nil
}

func (s *Server) StepIn(ctx context.Context, request *wirev1.StepInRequest) (*wirev1.StepInResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.StepIn(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepInResponse{}, nil
}

func (s *Server) StepOut(ctx context.Context, request *wirev1.StepOutRequest) (*wirev1.StepOutResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.StepOut(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepOutResponse{}, nil
}

func (s *Server) Terminate(ctx context.Context, request *wirev1.TerminateRequest) (*wirev1.TerminateResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Stop(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.TerminateResponse{}, nil
}

func (s *Server) SetBreakpoint(ctx context.Context, request *wirev1.SetBreakpointRequest) (*wirev1.SetBreakpointResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	location, err := sourceLocationFromProto(request.GetLocation(), "breakpoint location")
	if err != nil {
		return nil, rpcError(err)
	}

	options, err := breakpointOptions(request.GetOptions())
	if err != nil {
		return nil, rpcError(err)
	}

	value, err := session.SetBreakpointAt(operation, location, options)
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
	operation, cancel, session, err := s.debugCommand(ctx, request)
	if err != nil {
		return nil, err
	}

	defer cancel()

	id, err := debuggerIDFromProto[debugger.BreakpointID](request.GetBreakpointId(), "breakpoint ID")
	if err != nil {
		return nil, rpcError(err)
	}

	if err := session.DeleteBreakpoint(operation, id); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.DeleteBreakpointResponse{}, nil
}

func (s *Server) ReleaseDebugSession(
	ctx context.Context,
	request *wirev1.ReleaseDebugSessionRequest,
) (*wirev1.ReleaseDebugSessionResponse, error) {
	operation, cancel, err := s.operationContext(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := s.lifecycle.ReleaseDebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseDebugSessionResponse{}, nil
}
