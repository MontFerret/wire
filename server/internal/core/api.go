package core

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

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

func closeAPIDebugSession(session debugger.Session) error {
	return runtimePanicError("close runtime debug session", panicboundary.Do(session.Close))
}

func runtimePanicError(operation string, err error) error {
	var panicErr *panicboundary.Error
	if !errors.As(err, &panicErr) {
		return err
	}

	return internalError(fmt.Errorf("%s: %w", operation, err))
}

func isNil(value any) bool {
	if value == nil {
		return true
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
