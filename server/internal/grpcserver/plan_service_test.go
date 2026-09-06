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
		want    api.OptimizationLevel
		present bool
	}{
		{name: "missing options"},
		{name: "unspecified", options: &wirev1.CompileOptions{}},
		{name: "none", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_NONE), want: api.OptimizationNone, present: true},
		{name: "basic", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_BASIC), want: api.OptimizationBasic, present: true},
		{name: "full", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_FULL), want: api.OptimizationFull, present: true},
		{name: "aggressive", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_AGGRESSIVE), want: api.OptimizationAggressive, present: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, present, err := optimizationLevel(test.options)
			if err != nil {
				t.Fatal(err)
			}

			if got != test.want || present != test.present {
				t.Fatalf("optimization = (%v, %v), want (%v, %v)", got, present, test.want, test.present)
			}
		})
	}
}

func TestOptimizationLevelRejectsUnknownValue(t *testing.T) {
	_, _, err := optimizationLevel(compileOptions(wirev1.OptimizationLevel(99)))

	var domain *core.DomainError
	if !errors.As(err, &domain) || domain.Kind != core.ErrorKindInvalidRequest {
		t.Fatalf("unexpected invalid optimization result: %v", err)
	}
}

func compileOptions(level wirev1.OptimizationLevel) *wirev1.CompileOptions {
	return &wirev1.CompileOptions{OptimizationLevel: level}
}
