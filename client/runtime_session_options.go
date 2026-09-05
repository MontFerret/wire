package client

import (
	"errors"

	"github.com/MontFerret/api"
)

type runtimeSessionOptions struct {
	parameters        map[string]any
	outputContentType string
}

func applyRuntimeSessionOptions(options []api.SessionOption) (runtimeSessionOptions, error) {
	configured := runtimeSessionOptions{parameters: make(map[string]any)}
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
