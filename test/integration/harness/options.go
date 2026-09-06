package harness

import "github.com/MontFerret/api"

type (
	// CompileOptions preserves optimization-level presence separately from its value.
	CompileOptions struct {
		Level    api.OptimizationLevel
		HasLevel bool
	}

	// SessionOptions records parameters and output format supplied through API options.
	SessionOptions struct {
		Params      map[string]any
		ContentType string
	}
)

// SetOptimizationLevel records both the value and explicit option presence.
func (o *CompileOptions) SetOptimizationLevel(level api.OptimizationLevel) error {
	o.Level, o.HasLevel = level, true

	return nil
}

// SetParam records one supplied parameter without changing its value type.
func (o *SessionOptions) SetParam(name string, value any) error {
	if o.Params == nil {
		o.Params = make(map[string]any)
	}

	o.Params[name] = value

	return nil
}

// SetParams merges the supplied parameters into the recorded option state.
func (o *SessionOptions) SetParams(values map[string]any) error {
	for name, value := range values {
		if err := o.SetParam(name, value); err != nil {
			return err
		}
	}

	return nil
}

// SetOutputContentType records the requested output format verbatim.
func (o *SessionOptions) SetOutputContentType(value string) error {
	o.ContentType = value

	return nil
}

func (o SessionOptions) clone() SessionOptions {
	if o.Params != nil {
		o.Params = cloneValue(o.Params).(map[string]any)
	}

	return o
}

// cloneValue copies portable API values without a JSON numeric conversion.
func cloneValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(value))

		for key, item := range value {
			result[key] = cloneValue(item)
		}

		return result
	case []any:
		result := make([]any, len(value))

		for index, item := range value {
			result[index] = cloneValue(item)
		}

		return result
	case []byte:
		return append([]byte(nil), value...)
	default:
		return value
	}
}

func applyOptions(options []api.SessionOption) (SessionOptions, error) {
	configured := SessionOptions{Params: make(map[string]any)}

	for _, option := range options {
		if option != nil {
			if err := option(&configured); err != nil {
				return SessionOptions{}, err
			}
		}
	}

	return configured, nil
}
