package core

import (
	"errors"
	"fmt"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

func apiSessionOptions(parameters map[string]any, contentType string) []api.SessionOption {
	options := []api.SessionOption{api.WithParams(cloneParameters(parameters))}
	if contentType != "" {
		options = append(options, api.WithOutputContentType(contentType))
	}

	return options
}

func apiPlanParameters(plan api.Plan) ([]string, error) {
	parameters, err := panicboundary.Call(func() ([]string, error) {
		return plan.Params(), nil
	})
	if err != nil {
		return nil, runtimePanicError("read runtime plan parameters", err)
	}

	return append([]string(nil), parameters...), nil
}

func closeAPIPlan(plan api.Plan) error {
	return runtimePanicError("close runtime plan", panicboundary.Do(plan.Close))
}

func closeAPISession(session api.Session) error {
	return runtimePanicError("close runtime session", panicboundary.Do(session.Close))
}

func runtimePanicError(operation string, err error) error {
	var panicErr *panicboundary.Error
	if !errors.As(err, &panicErr) {
		return err
	}

	return internalError(fmt.Errorf("%s: %w", operation, err))
}
