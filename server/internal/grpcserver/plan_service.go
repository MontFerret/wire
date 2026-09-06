package grpcserver

import (
	"context"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// PlanService adapts the plan RPC contract to its core owners.
type PlanService struct {
	wirev1.UnimplementedPlanServiceServer
	runtime     api.Runtime
	connections *core.ConnectionRegistry
}

var _ wirev1.PlanServiceServer = (*PlanService)(nil)

func (s *PlanService) Compile(ctx context.Context, request *wirev1.CompileRequest) (*wirev1.CompileResponse, error) {
	compiled, err := s.compile(ctx, request.GetConnectionId(), request.GetSource(), request.GetOptions(), false)
	if err != nil {
		return nil, err
	}

	return &wirev1.CompileResponse{Plan: compiled}, nil
}

func (s *PlanService) CompileDebug(ctx context.Context, request *wirev1.CompileDebugRequest) (*wirev1.CompileDebugResponse, error) {
	compiled, err := s.compile(ctx, request.GetConnectionId(), request.GetSource(), request.GetOptions(), true)
	if err != nil {
		return nil, err
	}

	return &wirev1.CompileDebugResponse{Plan: compiled}, nil
}

func (s *PlanService) compile(
	ctx context.Context,
	connectionID *wirev1.ConnectionId,
	source *wirev1.Source,
	options *wirev1.CompileOptions,
	debug bool,
) (*wirev1.Plan, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, connectionID)
	if err != nil {
		return nil, err
	}

	defer cancel()

	planOptions, err := decodeCompileOptions(options)
	if err != nil {
		return nil, rpcError(err)
	}

	compiled, err := core.CompilePlan(operation, s.runtime, resources, decodeSource(source), debug, planOptions...)
	if err != nil {
		return nil, rpcError(err)
	}

	return plan(compiled), nil
}

func (s *PlanService) ReleasePlan(ctx context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := resources.ReleasePlan(operation, core.PlanID(request.GetPlanId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleasePlanResponse{}, nil
}
