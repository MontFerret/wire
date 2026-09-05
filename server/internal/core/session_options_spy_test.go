package core

import (
	"github.com/MontFerret/api"
)

func applySessionOptions(options []api.SessionOption) (sessionOptions, error) {
	configured := sessionOptions{params: make(map[string]any)}
	for _, option := range options {
		if err := option(&configured); err != nil {
			return sessionOptions{}, err
		}
	}

	return configured, nil
}

type sessionOptions struct {
	params      map[string]any
	contentType string
}

func (o *sessionOptions) SetParam(name string, value any) error {
	if o.params == nil {
		o.params = make(map[string]any)
	}

	o.params[name] = cloneParameter(value)

	return nil
}

func (o *sessionOptions) SetParams(values map[string]any) error {
	if o.params == nil {
		o.params = make(map[string]any)
	}

	for name, value := range values {
		o.params[name] = cloneParameter(value)
	}

	return nil
}

func (o *sessionOptions) SetOutputContentType(contentType string) error {
	o.contentType = contentType

	return nil
}

func (o sessionOptions) clone() sessionOptions {
	o.params = cloneParameters(o.params)

	return o
}
