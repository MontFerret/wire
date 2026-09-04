package client

import (
	"errors"
	"fmt"

	"github.com/MontFerret/api"
)

type (
	runtimePlanOptions struct {
		optimizationLevel *api.OptimizationLevel
	}

	runtimeSessionOptions struct {
		parameters        Parameters
		outputContentType string
	}
)

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
		copied := level
		o.optimizationLevel = &copied

		return nil
	default:
		return fmt.Errorf("invalid optimization level: %d", level)
	}
}

func applyRuntimeSessionOptions(options []api.SessionOption) (runtimeSessionOptions, error) {
	configured := runtimeSessionOptions{parameters: make(Parameters)}
	var result error
	for _, option := range options {
		if option == nil {
			continue
		}

		result = errors.Join(result, option(&configured))
	}

	_, validationErr := encodeParameters(configured.parameters)
	result = errors.Join(result, validationErr)

	return configured, result
}

func (o *runtimeSessionOptions) SetParam(name string, value any) error {
	if name == "" {
		return errors.New("parameter name must not be empty")
	}

	o.parameters[name] = value

	return nil
}

func (o *runtimeSessionOptions) SetParams(values map[string]any) error {
	for name, value := range values {
		if name == "" {
			return errors.New("parameter name must not be empty")
		}

		o.parameters[name] = value
	}

	return nil
}

func (o *runtimeSessionOptions) SetOutputContentType(contentType string) error {
	o.outputContentType = contentType

	return nil
}
