package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// SessionService adapts the session RPC contract to its core owners.
type SessionService struct {
	wirev1.UnimplementedSessionServiceServer
	executor   *core.Executor
	lifecycle  *core.Lifecycle
	operations *operationContextFactory
}

var _ wirev1.SessionServiceServer = (*SessionService)(nil)

func (s *SessionService) CreateSession(
	ctx context.Context,
	request *wirev1.CreateSessionRequest,
) (*wirev1.CreateSessionResponse, error) {
	operation, cancel, err := s.operations.New(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	parameters, err := decodeParameters(request.GetParameters())
	if err != nil {
		return nil, rpcError(&core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: err.Error()})
	}

	id, err := s.executor.CreateSession(operation, core.CreateSessionInput{
		PlanID:            core.PlanID(request.GetPlanId().GetValue()),
		Parameters:        parameters,
		OutputContentType: request.GetOutputContentType(),
	})
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.CreateSessionResponse{Session: &wirev1.Session{
		Id: &wirev1.SessionId{Value: string(id)},
	}}, nil
}

func (s *SessionService) ReleaseSession(
	ctx context.Context,
	request *wirev1.ReleaseSessionRequest,
) (*wirev1.ReleaseSessionResponse, error) {
	operation, cancel, err := s.operations.New(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := s.lifecycle.ReleaseSession(operation, core.SessionID(request.GetSessionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseSessionResponse{}, nil
}
