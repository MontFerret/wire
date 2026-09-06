package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func plan(value *core.Plan) *wirev1.Plan {
	return &wirev1.Plan{
		Id:         &wirev1.PlanId{Value: string(value.ID())},
		Parameters: value.Params(),
	}
}
