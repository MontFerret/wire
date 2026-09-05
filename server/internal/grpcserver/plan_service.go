package grpcserver

import (
	"context"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

type compileRequest interface {
	GetConnectionId() *wirev1.ConnectionId
	GetSource() *wirev1.Source
	GetOptions() *wirev1.CompileOptions
}

func (s *Server) Compile(ctx context.Context, request *wirev1.CompileRequest) (*wirev1.CompileResponse, error) {
	compiled, err := s.compile(ctx, request, false)
	if err != nil {
		return nil, err
	}

	return &wirev1.CompileResponse{Plan: compiled}, nil
}

func (s *Server) CompileDebug(ctx context.Context, request *wirev1.CompileDebugRequest) (*wirev1.CompileDebugResponse, error) {
	compiled, err := s.compile(ctx, request, true)
	if err != nil {
		return nil, err
	}

	return &wirev1.CompileDebugResponse{Plan: compiled}, nil
}

func (s *Server) compile(ctx context.Context, request compileRequest, debug bool) (*wirev1.Plan, error) {
	operation, cancel, err := s.operationContext(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	optimization, present, err := optimizationLevel(request.GetOptions())
	if err != nil {
		return nil, rpcError(err)
	}

	snapshot, err := s.compiler.Compile(operation, core.CompileInput{
		Source: api.Source{
			Name:    request.GetSource().GetName(),
			Content: request.GetSource().GetContent(),
		},
		Debuggable:           debug,
		OptimizationLevel:    optimization,
		HasOptimizationLevel: present,
	})
	if err != nil {
		return nil, rpcError(err)
	}

	return plan(snapshot), nil
}

func optimizationLevel(options *wirev1.CompileOptions) (api.OptimizationLevel, bool, error) {
	value := wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_UNSPECIFIED
	if options != nil {
		value = options.GetOptimizationLevel()
	}

	var level api.OptimizationLevel
	switch value {
	case wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_UNSPECIFIED:
		return 0, false, nil
	case wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_NONE:
		level = api.OptimizationNone
	case wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_BASIC:
		level = api.OptimizationBasic
	case wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_FULL:
		level = api.OptimizationFull
	case wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_AGGRESSIVE:
		level = api.OptimizationAggressive
	default:
		return 0, false, &core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: "optimization level is invalid"}
	}

	return level, true, nil
}

func (s *Server) ReleasePlan(ctx context.Context, request *wirev1.ReleasePlanRequest) (*wirev1.ReleasePlanResponse, error) {
	operation, cancel, err := s.operationContext(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := s.lifecycle.ReleasePlan(operation, core.PlanID(request.GetPlanId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleasePlanResponse{}, nil
}
