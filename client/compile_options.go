package client

import (
	"errors"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// CompileOptions controls runtime plan construction.
type CompileOptions struct {
	Debuggable bool

	// PlanOptions are applied once, in order, before dispatch. Omitting an
	// optimization option preserves the hosted runtime's default, while
	// api.WithOptimizationLevel(api.OptimizationNone) explicitly disables it.
	PlanOptions []api.PlanOption
}

func encodeCompileOptions(level api.OptimizationLevel, present bool) (*wirev1.CompileOptions, error) {
	if !present {
		return nil, nil
	}

	var converted wirev1.OptimizationLevel
	switch level {
	case api.OptimizationNone:
		converted = wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_NONE
	case api.OptimizationBasic:
		converted = wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_BASIC
	case api.OptimizationFull:
		converted = wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_FULL
	case api.OptimizationAggressive:
		converted = wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_AGGRESSIVE
	default:
		return nil, errors.New("invalid optimization level")
	}

	return &wirev1.CompileOptions{OptimizationLevel: converted}, nil
}
