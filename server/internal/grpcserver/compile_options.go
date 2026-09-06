package grpcserver

import (
	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

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

func decodeCompileOptions(options *wirev1.CompileOptions) ([]api.PlanOption, error) {
	level, present, err := optimizationLevel(options)
	if err != nil {
		return nil, err
	}

	if !present {
		return nil, nil
	}

	return []api.PlanOption{api.WithOptimizationLevel(level)}, nil
}
