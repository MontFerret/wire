package client

import (
	"errors"
	"fmt"

	"github.com/MontFerret/api"
)

type runtimePlanOptions struct {
	optimizationLevel    api.OptimizationLevel
	hasOptimizationLevel bool
}

func applyRuntimePlanOptions(options []api.PlanOption) (runtimePlanOptions, error) {
	configured := runtimePlanOptions{}
	var result error
	for _, option := range options {
		if option == nil {
			continue
		}

		result = errors.Join(result, option(&configured))
	}

	return configured, result
}

func (o *runtimePlanOptions) SetOptimizationLevel(level api.OptimizationLevel) error {
	switch level {
	case api.OptimizationNone, api.OptimizationBasic, api.OptimizationFull, api.OptimizationAggressive:
		o.optimizationLevel = level
		o.hasOptimizationLevel = true

		return nil
	default:
		return fmt.Errorf("invalid optimization level: %d", level)
	}
}
