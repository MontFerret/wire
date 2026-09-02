package core

import (
	"errors"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
)

type apiResource interface {
	Close() error
}

func closeAPIResource(resource apiResource, panicMessage string) (err error) {
	defer func() {
		if recover() != nil {
			err = internalError(errors.New(panicMessage))
		}
	}()

	return resource.Close()
}

func closeAPIPlan(plan api.Plan) error {
	return closeAPIResource(plan, "runtime plan cleanup panicked")
}

func closeAPISession(session api.Session) error {
	return closeAPIResource(session, "runtime session cleanup panicked")
}

func closeAPIDebugSession(session debugger.Session) error {
	return closeAPIResource(session, "runtime debug cleanup panicked")
}
