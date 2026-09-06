package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// SessionService adapts the session RPC contract to its core owners.
type SessionService struct {
	wirev1.UnimplementedSessionServiceServer
	connections *core.ConnectionRegistry
}

var _ wirev1.SessionServiceServer = (*SessionService)(nil)

// CreateSession applies decoded options to a durable session under the requested plan.
func (s *SessionService) CreateSession(
	ctx context.Context,
	request *wirev1.CreateSessionRequest,
) (*wirev1.CreateSessionResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	options, err := decodeSessionOptions(request.GetParameters(), request.GetOutputContentType())
	if err != nil {
		return nil, rpcError(err)
	}

	parent, err := resources.Plan(operation, core.PlanID(request.GetPlanId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	created, err := parent.NewSession(operation, options...)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.CreateSessionResponse{Session: &wirev1.Session{
		Id: &wirev1.SessionId{Value: string(created.ID())},
	}}, nil
}

// ReleaseSession reclaims a durable session through its logical connection.
func (s *SessionService) ReleaseSession(
	ctx context.Context,
	request *wirev1.ReleaseSessionRequest,
) (*wirev1.ReleaseSessionResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := resources.ReleaseSession(operation, core.SessionID(request.GetSessionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseSessionResponse{}, nil
}
