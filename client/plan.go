package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	// CompileOptions controls runtime plan construction.
	CompileOptions struct {
		Debuggable bool
	}

	// Plan is a compiled remote runtime plan owned by one Client.
	Plan struct {
		client     *Client
		id         string
		parameters []string
		debuggable bool
		close      *lifecycle.Close
	}
)

// Compile creates a connection-owned plan through the hosted runtime.
func (c *Client) Compile(ctx context.Context, src api.Source, options CompileOptions) (*Plan, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	var value *wirev1.Plan
	if options.Debuggable {
		response, err := c.planClient.CompileDebug(ctx, &wirev1.CompileDebugRequest{
			ConnectionId: c.connectionProto(),
			Source:       &wirev1.Source{Content: src.Content, Name: src.Name},
		})
		if err != nil {
			return nil, decodeError(err)
		}

		value = response.GetPlan()
	} else {
		response, err := c.planClient.Compile(ctx, &wirev1.CompileRequest{
			ConnectionId: c.connectionProto(),
			Source:       &wirev1.Source{Content: src.Content, Name: src.Name},
		})
		if err != nil {
			return nil, decodeError(err)
		}

		value = response.GetPlan()
	}

	if value == nil || value.GetId().GetValue() == "" {
		return nil, errors.New("Wire server returned an invalid compiled plan")
	}

	return &Plan{
		client:     c,
		id:         value.GetId().GetValue(),
		parameters: append([]string(nil), value.GetParameters()...),
		debuggable: options.Debuggable,
		close:      &lifecycle.Close{},
	}, nil
}

// Parameters returns a copy of the FQL parameters declared by this plan.
func (p *Plan) Parameters() []string {
	if p == nil {
		return nil
	}

	return append([]string(nil), p.parameters...)
}

// Debuggable reports whether this plan was compiled for debugging.
func (p *Plan) Debuggable() bool {
	return p != nil && p.debuggable
}

// Run executes the plan once, waits for encoded output, and releases the
// execution it creates. Execution and release errors are joined. The caller
// retains ownership of the Plan.
func (p *Plan) Run(ctx context.Context, parameters Parameters, options ExecuteOptions) (Output, error) {
	execution, err := p.Execute(ctx, parameters, options)
	if err != nil {
		return Output{}, err
	}

	output, waitErr := execution.Wait(ctx)
	closeErr := execution.Close(context.WithoutCancel(ctx))

	return output, errors.Join(waitErr, closeErr)
}

// Close releases the plan and its remote executions and debug sessions.
// Concurrent and repeated calls observe one retained release result.
func (p *Plan) Close(ctx context.Context) error {
	if p == nil || p.client == nil || p.id == "" || p.close == nil {
		return ErrClosed
	}

	if p.close.Begin() {
		go settleHandleClose(ctx, "plan", p.close, p.release)
	}

	return p.close.Wait(ctx)
}

func (p *Plan) checkOpen() error {
	if p == nil || p.client == nil || p.id == "" || p.close == nil || p.close.Started() {
		return ErrClosed
	}

	return p.client.checkOpen()
}

func (p *Plan) ancestorCloseResult(ctx context.Context) (bool, error) {
	if p == nil || p.client == nil || p.close == nil {
		return true, ErrClosed
	}

	if p.close.Started() {
		return true, p.close.Wait(ctx)
	}

	return p.client.closeResult(ctx)
}

func (p *Plan) release(ctx context.Context) error {
	if closing, err := p.client.closeResult(ctx); closing {
		return err
	}

	return p.client.releasePlan(ctx, p.id)
}

func (c *Client) releasePlan(ctx context.Context, id string) error {
	if err := c.checkOpen(); err != nil {
		return err
	}

	_, err := c.planClient.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{
		ConnectionId: c.connectionProto(),
		PlanId:       &wirev1.PlanId{Value: id},
	})

	return decodeError(err)
}
