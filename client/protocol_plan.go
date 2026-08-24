package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

type compiledPlan struct {
	id         string
	parameters []string
	debuggable bool
}

func (p *protocolClient) compile(ctx context.Context, source Source, options CompileOptions) (compiledPlan, error) {
	response, err := p.planClient.Compile(ctx, &wirev1.CompileRequest{
		ConnectionId: p.connectionProto(),
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

func (p *protocolClient) releasePlan(ctx context.Context, id string) error {
	_, err := p.planClient.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{
		ConnectionId: p.connectionProto(),
		PlanId:       &wirev1.PlanId{Value: id},
	})

	return decodeError(err)
}
