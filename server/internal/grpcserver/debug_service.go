package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// DebugService adapts debugger lifecycle, commands, inspection, and events.
type DebugService struct {
	wirev1.UnimplementedDebugServiceServer
	connections *core.ConnectionRegistry
}

var _ wirev1.DebugServiceServer = (*DebugService)(nil)

// CreateDebugSession decodes options and creates a debugger under the requested plan.
func (s *DebugService) CreateDebugSession(
	ctx context.Context,
	request *wirev1.CreateDebugSessionRequest,
) (*wirev1.CreateDebugSessionResponse, error) {
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

	created, err := parent.NewDebugSession(operation, options...)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := debugSession(created.ID(), created.Snapshot())
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.CreateDebugSessionResponse{Session: converted}, nil
}

// ReleaseDebugSession reclaims a debugger through its logical connection.
func (s *DebugService) ReleaseDebugSession(
	ctx context.Context,
	request *wirev1.ReleaseDebugSessionRequest,
) (*wirev1.ReleaseDebugSessionResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := resources.ReleaseDebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseDebugSessionResponse{}, nil
}
