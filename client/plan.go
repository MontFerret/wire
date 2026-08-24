package client

import (
	"context"
	"errors"

	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	// Source is FQL content plus its diagnostic and debugger identity.
	Source struct {
		Content  string
		Identity string
	}

	// CompileOptions controls Ferret plan construction.
	CompileOptions struct {
		Debuggable bool
	}

	// Plan is a compiled remote Ferret program owned by one Client.
	Plan struct {
		client     *Client
		transport  *planTransport
		executions *executionTransport
		debug      *debugTransport
		id         string
		parameters []string
		debuggable bool
		handle     *lifecycle.Handle
	}
)

func newPlan(client *Client, compiled compiledPlan) *Plan {
	return &Plan{
		client:     client,
		transport:  client.plans,
		executions: client.executions,
		debug:      client.debug,
		id:         compiled.id,
		parameters: compiled.parameters,
		debuggable: compiled.debuggable,
		handle:     &lifecycle.Handle{},
	}
}

// Compile creates a connection-owned plan through Ferret's public compiler.
// Compilation diagnostics are returned through Error.
func (c *Client) Compile(ctx context.Context, source Source, options CompileOptions) (*Plan, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}

	compiled, err := c.plans.compile(ctx, source, options)
	if err != nil {
		return nil, err
	}

	return newPlan(c, compiled), nil
}

// Execute publishes a remote execution of this plan. Output remains Ferret's
// encoded content-type and byte contract.
func (p *Plan) Execute(ctx context.Context, parameters Parameters, options ExecuteOptions) (*Execution, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	id, err := p.executions.execute(ctx, p.id, parameters, options)
	if err != nil {
		return nil, err
	}

	return newExecution(p, p.executions, id), nil
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
	if p == nil || p.client == nil || p.transport == nil || p.id == "" || p.handle == nil {
		return ErrClosed
	}

	return p.handle.Close(ctx, p.release)
}

func (p *Plan) checkOpen() error {
	if p == nil || p.client == nil || p.transport == nil || p.executions == nil || p.debug == nil ||
		p.id == "" || p.handle == nil || !p.handle.Open() {
		return ErrClosed
	}

	return p.client.checkOpen()
}

func (p *Plan) ancestorCloseResult(ctx context.Context) (bool, error) {
	if p == nil || p.client == nil || p.handle == nil {
		return true, ErrClosed
	}

	if closing, err := p.handle.CloseResult(ctx); closing {
		return true, err
	}

	return p.client.closeResult(ctx)
}

func (p *Plan) release(ctx context.Context) error {
	if closing, err := p.client.closeResult(ctx); closing {
		return err
	}

	if err := p.client.checkOpen(); err != nil {
		return err
	}

	return p.transport.release(ctx, p.id)
}
