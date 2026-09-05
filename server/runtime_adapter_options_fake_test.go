package server_test

import (
	"github.com/MontFerret/api"
)

type contractPlanOptions struct {
	optimizationLevel    api.OptimizationLevel
	hasOptimizationLevel bool
}

func (o *contractPlanOptions) SetOptimizationLevel(level api.OptimizationLevel) error {
	o.optimizationLevel = level
	o.hasOptimizationLevel = true

	return nil
}

func applyAPIOptions(options []api.SessionOption) (apiSessionOptions, error) {
	configured := apiSessionOptions{params: make(map[string]any)}
	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(&configured); err != nil {
			return apiSessionOptions{}, err
		}
	}

	return configured, nil
}

func cloneOptimizationLevels(values []contractPlanOptions) []api.OptimizationLevel {
	result := make([]api.OptimizationLevel, len(values))
	for index, value := range values {
		result[index] = value.optimizationLevel
	}

	return result
}
