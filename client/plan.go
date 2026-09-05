package client

import (
	"context"
	"errors"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// Plan is a compiled remote runtime plan owned by one Client.
type Plan struct {
	client     *Client
	id         string
	parameters []string
	debuggable bool
	close      *closeState
}

// Execute publishes a remote execution of this plan. Output remains the Unified API
// encoded content-type and byte contract.
func (p *Plan) Execute(ctx context.Context, parameters Parameters, options ExecuteOptions) (*Execution, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.executionClient.Execute(ctx, &wirev1.ExecuteRequest{
		ConnectionId:      p.client.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: p.id},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return nil, decodeError(err)
	}

	return newExecutionHandle(p.client, p, nil, response.GetExecution())
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
func (p *Plan) Run(ctx context.Context, parameters Parameters, options ExecuteOptions) (api.Output, error) {
	execution, err := p.Execute(ctx, parameters, options)
	if err != nil {
		return api.Output{}, err
	}

	output, waitErr := execution.Wait(ctx)
	closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, execution.Close)

	return output, errors.Join(waitErr, closeErr)
}

// NewDebugSession creates a Unified API debug session for a plan compiled with
// CompileOptions.Debuggable.
func (p *Plan) NewDebugSession(ctx context.Context, parameters Parameters, options DebugSessionOptions) (*DebugSession, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.debugClient.CreateDebugSession(ctx, &wirev1.CreateDebugSessionRequest{
		ConnectionId:      p.client.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: p.id},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return nil, allocationRPCError(err)
	}

	value := response.GetSession()
	if value == nil || value.GetId().GetValue() == "" {
		return nil, &allocationError{cause: errors.New("Wire server returned an invalid debug session")}
	}

	return &DebugSession{client: p.client, plan: p, id: value.GetId().GetValue(), close: &closeState{}}, nil
}

// Close releases the plan and its remote sessions, executions, and debug sessions.
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

	if err := p.client.checkOpen(); err != nil {
		return err
	}

	_, err := p.client.planClient.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{
		ConnectionId: p.client.connectionProto(),
		PlanId:       &wirev1.PlanId{Value: p.id},
	})

	return decodeError(err)
}

func (p *Plan) newSession(
	ctx context.Context,
	parameters Parameters,
	options ExecuteOptions,
) (*sessionHandle, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.sessionClient.CreateSession(ctx, &wirev1.CreateSessionRequest{
		ConnectionId:      p.client.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: p.id},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return nil, allocationRPCError(err)
	}

	value := response.GetSession()
	if value == nil || value.GetId().GetValue() == "" {
		return nil, &allocationError{cause: errors.New("Wire server returned an invalid session")}
	}

	return &sessionHandle{
		client: p.client,
		plan:   p,
		id:     value.GetId().GetValue(),
		close:  &closeState{},
	}, nil
}
