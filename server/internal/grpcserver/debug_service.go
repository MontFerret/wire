package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// DebugService adapts debugger lifecycle, commands, inspection, and events.
type DebugService struct {
	wirev1.UnimplementedDebugServiceServer
	debugger   *core.Debugger
	lifecycle  *core.Lifecycle
	operations *operationContextFactory
}

var _ wirev1.DebugServiceServer = (*DebugService)(nil)

func (s *DebugService) CreateDebugSession(
	ctx context.Context,
	request *wirev1.CreateDebugSessionRequest,
) (*wirev1.CreateDebugSessionResponse, error) {
	operation, cancel, err := s.operations.New(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	parameters, err := decodeParameters(request.GetParameters())
	if err != nil {
		return nil, rpcError(&core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: err.Error()})
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

func (s *DebugService) ReleaseDebugSession(
	ctx context.Context,
	request *wirev1.ReleaseDebugSessionRequest,
) (*wirev1.ReleaseDebugSessionResponse, error) {
	operation, cancel, err := s.operations.New(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := s.lifecycle.ReleaseDebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseDebugSessionResponse{}, nil
}
