package client

import (
	"errors"
	"strings"
	"testing"

	"github.com/MontFerret/api"
)

func TestRuntimePlanOptionsApplyAllNonNilOptionsInOrder(t *testing.T) {
	var order []int
	first := api.PlanOption(func(options api.PlanOptions) error {
		order = append(order, 1)

		return options.SetOptimizationLevel(api.OptimizationBasic)
	})
	second := api.PlanOption(func(options api.PlanOptions) error {
		order = append(order, 2)

		return options.SetOptimizationLevel(api.OptimizationFull)
	})

	configured, err := applyRuntimePlanOptions([]api.PlanOption{first, nil, second})
	if err != nil {
		t.Fatal(err)
	}

	if !configured.hasOptimizationLevel || configured.optimizationLevel != api.OptimizationFull {
		t.Fatalf("last option did not win: %#v", configured.optimizationLevel)
	}

	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("options were not applied in order: %#v", order)
	}
}

func TestRuntimeOptionsAggregateFailuresAndPortableValidation(t *testing.T) {
	firstErr := errors.New("first option failed")
	secondErr := errors.New("second option failed")
	var calls int
	configured, err := applyRuntimeSessionOptions([]api.SessionOption{
		func(options api.SessionOptions) error {
			calls++

			if setErr := options.SetParam("unsupported", make(chan struct{})); setErr != nil {
				return errors.Join(firstErr, setErr)
			}

			return firstErr
		},
		nil,
		func(options api.SessionOptions) error {
			calls++

			if setErr := options.SetOutputContentType("application/json"); setErr != nil {
				return errors.Join(secondErr, setErr)
			}

			return secondErr
		},
	})

	if calls != 2 {
		t.Fatalf("not all non-nil options were applied: %d", calls)
	}

	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("option and portability failures were not aggregated: %v", err)
	}

	if configured.outputContentType != "application/json" {
		t.Fatalf("later option was not applied after an earlier failure: %#v", configured)
	}
}

func TestRuntimeSessionOptionsRejectEmptyParameterNamesLocally(t *testing.T) {
	_, err := applyRuntimeSessionOptions([]api.SessionOption{api.WithParam("", int64(1))})
	if err == nil || !strings.Contains(err.Error(), "parameter name") {
		t.Fatalf("empty parameter name was accepted: %v", err)
	}
}

func TestRuntimePlanOptionsPreserveAbsenceAndExplicitZero(t *testing.T) {
	unset, err := applyRuntimePlanOptions(nil)
	if err != nil || unset.hasOptimizationLevel {
		t.Fatalf("missing optimization became present: %+v, %v", unset, err)
	}

	explicit, err := applyRuntimePlanOptions([]api.PlanOption{api.WithOptimizationLevel(api.OptimizationNone)})
	if err != nil || !explicit.hasOptimizationLevel || explicit.optimizationLevel != api.OptimizationNone {
		t.Fatalf("explicit zero optimization was lost: %+v, %v", explicit, err)
	}

	if err := explicit.SetOptimizationLevel(api.OptimizationLevel(99)); err == nil {
		t.Fatal("invalid optimization was accepted")
	}

	if !explicit.hasOptimizationLevel || explicit.optimizationLevel != api.OptimizationNone {
		t.Fatal("invalid optimization replaced the last valid option")
	}
}
