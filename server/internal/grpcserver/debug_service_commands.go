package grpcserver

import (
	"context"

	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func (s *DebugService) debugCommand(
	ctx context.Context,
	connectionID *wirev1.ConnectionId,
	sessionID *wirev1.DebugSessionId,
) (context.Context, context.CancelFunc, *core.DebugSession, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, connectionID)
	if err != nil {
		return nil, nil, nil, err
	}

	session, err := resources.DebugSession(operation, core.DebugSessionID(sessionID.GetValue()))
	if err != nil {
		cancel()

		return nil, nil, nil, rpcError(err)
	}

	return operation, cancel, session, nil
}

func (s *DebugService) Start(ctx context.Context, request *wirev1.StartRequest) (*wirev1.StartResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Start(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StartResponse{}, nil
}

func (s *DebugService) Continue(ctx context.Context, request *wirev1.ContinueRequest) (*wirev1.ContinueResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Continue(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ContinueResponse{}, nil
}

func (s *DebugService) Pause(ctx context.Context, request *wirev1.PauseRequest) (*wirev1.PauseResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Pause(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.PauseResponse{}, nil
}

func (s *DebugService) StepOver(ctx context.Context, request *wirev1.StepOverRequest) (*wirev1.StepOverResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.StepOver(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepOverResponse{}, nil
}

func (s *DebugService) StepIn(ctx context.Context, request *wirev1.StepInRequest) (*wirev1.StepInResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.StepIn(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepInResponse{}, nil
}

func (s *DebugService) StepOut(ctx context.Context, request *wirev1.StepOutRequest) (*wirev1.StepOutResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.StepOut(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.StepOutResponse{}, nil
}

func (s *DebugService) Terminate(ctx context.Context, request *wirev1.TerminateRequest) (*wirev1.TerminateResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if _, err := session.Stop(operation); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.TerminateResponse{}, nil
}

func (s *DebugService) SetBreakpoint(ctx context.Context, request *wirev1.SetBreakpointRequest) (*wirev1.SetBreakpointResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
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

func (s *DebugService) DeleteBreakpoint(ctx context.Context, request *wirev1.DeleteBreakpointRequest) (*wirev1.DeleteBreakpointResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
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
