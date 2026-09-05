package core

import (
	"github.com/MontFerret/api"
)

type CompileInput struct {
	Source               api.Source
	Debuggable           bool
	OptimizationLevel    api.OptimizationLevel
	HasOptimizationLevel bool
}
