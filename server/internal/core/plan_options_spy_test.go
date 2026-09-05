package core

import (
	"github.com/MontFerret/api"
)

type planOptions struct {
	optimizationLevel    api.OptimizationLevel
	hasOptimizationLevel bool
}

func (options *planOptions) SetOptimizationLevel(level api.OptimizationLevel) error {
	options.optimizationLevel = level
	options.hasOptimizationLevel = true

	return nil
}
