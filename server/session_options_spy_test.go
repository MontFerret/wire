package server_test

type apiSessionOptions struct {
	params      map[string]any
	contentType string
}

func (o *apiSessionOptions) SetParam(name string, value any) error {
	o.params[name] = value

	return nil
}

func (o *apiSessionOptions) SetParams(values map[string]any) error {
	for name, value := range values {
		o.params[name] = value
	}

	return nil
}

func (o *apiSessionOptions) SetOutputContentType(contentType string) error {
	o.contentType = contentType

	return nil
}

func (o apiSessionOptions) clone() apiSessionOptions {
	params := make(map[string]any, len(o.params))
	for name, value := range o.params {
		params[name] = value
	}

	o.params = params

	return o
}
