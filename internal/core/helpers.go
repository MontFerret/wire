package core

import (
	"errors"

	"github.com/MontFerret/api"
)

func closeAPISession(session api.Session) (err error) {
	defer func() {
		if recover() != nil {
			err = internalError(errors.New("runtime session cleanup panicked"))
		}
	}()

	return session.Close()
}

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

func apiPlanParameters(plan api.Plan) (parameters []string, err error) {
	defer func() {
		if recover() != nil {
			parameters = nil
			err = internalError(errors.New("runtime plan metadata panicked"))
		}
	}()

	return append([]string(nil), plan.Params()...), nil
}

func closeAPIPlan(plan api.Plan) (err error) {
	defer func() {
		if recover() != nil {
			err = internalError(errors.New("runtime plan cleanup panicked"))
		}
	}()

	return plan.Close()
}
