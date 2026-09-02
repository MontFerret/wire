package core

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

type apiResource interface {
	Close() error
}

func callAPI[T any](panicMessage string, call func() (T, error)) (result T, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			var zero T
			result = zero
			err = errors.New(panicMessage)
			panicked = true
		}
	}()

	result, err = call()

	return
}

func callAPIError(panicMessage string, call func() error) error {
	_, err, _ := callAPI(panicMessage, func() (struct{}, error) {
		return struct{}{}, call()
	})

	return err
}

func apiPlanParameters(plan api.Plan) ([]string, error) {
	parameters, err, panicked := callAPI("runtime plan metadata panicked", func() ([]string, error) {
		return append([]string(nil), plan.Params()...), nil
	})
	if panicked {
		return nil, internalError(err)
	}

	return parameters, nil
}

func openAPISession(
	ctx context.Context,
	plan api.Plan,
	options []api.SessionOption,
) (api.Session, error, bool) {
	return callAPI("runtime session creation panicked", func() (api.Session, error) {
		return plan.NewSession(ctx, options...)
	})
}

func runAPISession(ctx context.Context, session api.Session) (api.Output, error, bool) {
	return callAPI("runtime execution panicked", func() (api.Output, error) {
		return session.Run(ctx)
	})
}

func openAPIDebugSession(
	ctx context.Context,
	plan api.Plan,
	options []api.SessionOption,
) (debugger.Session, error, bool) {
	return callAPI("runtime debug session creation panicked", func() (debugger.Session, error) {
		return plan.NewDebugSession(ctx, options...)
	})
}

func closeAPIResource(resource apiResource, panicMessage string) error {
	_, err, panicked := callAPI(panicMessage, func() (struct{}, error) {
		return struct{}{}, resource.Close()
	})
	if panicked {
		return internalError(err)
	}

	return err
}

func closeAPIPlan(plan api.Plan) error {
	return closeAPIResource(plan, "runtime plan cleanup panicked")
}

func closeAPISession(session api.Session) error {
	return closeAPIResource(session, "runtime session cleanup panicked")
}
