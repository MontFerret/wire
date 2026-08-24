package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

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
