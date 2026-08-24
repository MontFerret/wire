package client

import (
	"context"
	"errors"
)

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

// Run compiles and executes source once, returning Ferret's encoded output.
// It releases the Plan and Execution resources it creates before returning and
// joins operation and release errors.
func (c *Client) Run(ctx context.Context, source Source, parameters Parameters, options RunOptions) (Output, error) {
	plan, err := c.Compile(ctx, source, options.Compile)
	if err != nil {
		return Output{}, err
	}

	output, runErr := plan.Run(ctx, parameters, options.Execute)
	closeErr := plan.Close(context.WithoutCancel(ctx))

	return output, errors.Join(runErr, closeErr)
}
