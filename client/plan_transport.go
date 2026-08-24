package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
)

type (
	// planTransport performs plan-specific protocol operations.
	planTransport struct {
		rpc     wirev1.PlanServiceClient
		session *session
	}

	compiledPlan struct {
		id         string
		parameters []string
		debuggable bool
	}
)

func newPlanTransport(connection grpc.ClientConnInterface, session *session) *planTransport {
	return &planTransport{rpc: wirev1.NewPlanServiceClient(connection), session: session}
}

func (t *planTransport) compile(ctx context.Context, source Source, options CompileOptions) (compiledPlan, error) {
	response, err := t.rpc.Compile(ctx, &wirev1.CompileRequest{
		ConnectionId: t.session.connectionProto(),
		Source:       &wirev1.Source{Content: source.Content, Identity: source.Identity},
		Options:      &wirev1.CompileOptions{Debuggable: options.Debuggable},
	})
	if err != nil {
		return compiledPlan{}, decodeError(err)
	}

	value := response.GetPlan()
	if value == nil || value.GetId().GetValue() == "" {
		return compiledPlan{}, errors.New("Wire server returned an invalid compiled plan")
	}

	return compiledPlan{
		id:         value.GetId().GetValue(),
		parameters: append([]string(nil), value.GetParameters()...),
		debuggable: value.GetDebuggable(),
	}, nil
}

func (t *planTransport) release(ctx context.Context, id string) error {
	_, err := t.rpc.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{
		ConnectionId: t.session.connectionProto(),
		PlanId:       &wirev1.PlanId{Value: id},
	})

	return decodeError(err)
}
