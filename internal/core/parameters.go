package core

func cloneParameters(values map[string]any) map[string]any {
	result := make(map[string]any, len(values))
	for name, value := range values {
		result[name] = cloneParameter(value)
	}

	return result
}

func cloneParameter(value any) any {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = cloneParameter(item)
		}

		return result
	case map[string]any:
		return cloneParameters(value)
	default:
		return value
	}
}
