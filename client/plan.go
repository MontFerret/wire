package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// Compile creates a connection-owned plan through Ferret's public compiler.
// Compilation diagnostics are returned through Error.
func (c *Client) Compile(ctx context.Context, source Source, options CompileOptions) (Plan, error) {
	if err := c.checkOpen(); err != nil {
		return Plan{}, err
	}

	response, err := c.planClient.Compile(ctx, &wirev1.CompileRequest{
		ConnectionId: c.connectionProto(),
		Source:       &wirev1.Source{Content: source.Content, Identity: source.Identity},
		Options:      &wirev1.CompileOptions{Debuggable: options.Debuggable},
	})
	if err != nil {
		return Plan{}, decodeError(err)
	}

	if response.GetPlan() == nil {
		return Plan{}, errors.New("Wire server returned no compiled plan")
	}

	return convertPlan(response.GetPlan()), nil
}

// ReleasePlan releases a plan and cascades through its executions and debug
// sessions. The ID becomes stale after cleanup completes.
func (c *Client) ReleasePlan(ctx context.Context, id PlanID) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	_, err := c.planClient.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{
		ConnectionId: c.connectionProto(),
		PlanId:       &wirev1.PlanId{Value: string(id)},
	})

	return decodeError(err)
}
