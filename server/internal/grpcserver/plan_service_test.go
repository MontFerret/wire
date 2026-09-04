package grpcserver

import (
	"errors"
	"testing"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func TestOptimizationLevelMapsPortableValuesAndPreservesRuntimeDefault(t *testing.T) {
	tests := []struct {
		name    string
		options *wirev1.CompileOptions
		want    *api.OptimizationLevel
	}{
		{name: "missing options"},
		{name: "unspecified", options: &wirev1.CompileOptions{}},
		{name: "none", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_NONE), want: apiOptimization(api.OptimizationNone)},
		{name: "basic", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_BASIC), want: apiOptimization(api.OptimizationBasic)},
		{name: "full", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_FULL), want: apiOptimization(api.OptimizationFull)},
		{name: "aggressive", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_AGGRESSIVE), want: apiOptimization(api.OptimizationAggressive)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := optimizationLevel(test.options)
			if err != nil {
				t.Fatal(err)
			}

			if test.want == nil {
				if got != nil {
					t.Fatalf("optimization = %v, want runtime default", *got)
				}

				return
			}

			if got == nil || *got != *test.want {
				t.Fatalf("optimization = %v, want %v", got, *test.want)
			}
		})
	}
}

func TestOptimizationLevelRejectsUnknownValue(t *testing.T) {
	_, err := optimizationLevel(compileOptions(wirev1.OptimizationLevel(99)))
	var domain *core.DomainError
	if !errors.As(err, &domain) || domain.Kind != core.ErrorKindInvalidRequest {
		t.Fatalf("unexpected invalid optimization result: %v", err)
	}
}

func compileOptions(level wirev1.OptimizationLevel) *wirev1.CompileOptions {
	return &wirev1.CompileOptions{OptimizationLevel: level}
}

func apiOptimization(level api.OptimizationLevel) *api.OptimizationLevel {
	return &level
}
