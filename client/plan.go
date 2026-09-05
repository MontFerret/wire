package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// planHandle is a compiled remote runtime plan owned by one connectionHandle.
type planHandle struct {
	client     *connectionHandle
	id         string
	parameters []string
	close      *closeState
}

// Parameters returns a copy of the FQL parameters declared by this plan.
func (p *planHandle) Parameters() []string {
	if p == nil {
		return nil
	}

	return append([]string(nil), p.parameters...)
}

// NewDebugSession creates a Unified API debug session for a plan compiled with
// CompileDebug.
func (p *planHandle) NewDebugSession(ctx context.Context, configured runtimeSessionOptions) (*debugSessionHandle, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(configured.parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.debugClient.CreateDebugSession(ctx, &wirev1.CreateDebugSessionRequest{
		ConnectionId:      p.client.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: p.id},
		Parameters:        converted,
		OutputContentType: configured.outputContentType,
	})
	if err != nil {
		return nil, allocationRPCError(err)
	}

	value := response.GetSession()
	if value == nil || value.GetId().GetValue() == "" {
		return nil, &allocationError{cause: errors.New("Wire server returned an invalid debug session")}
	}

	return &debugSessionHandle{client: p.client, plan: p, id: value.GetId().GetValue(), close: &closeState{}}, nil
}

// Close releases the plan and its remote sessions, executions, and debug sessions.
// Concurrent and repeated calls observe one retained release result.
func (p *planHandle) Close(ctx context.Context) error {
	if p == nil || p.client == nil || p.id == "" || p.close == nil {
		return ErrClosed
	}

	if p.close.Begin() {
		go settleHandleClose(ctx, "plan", p.close, p.release)
	}

	return p.close.Wait(ctx)
}

func (p *planHandle) checkOpen() error {
	if p == nil || p.client == nil || p.id == "" || p.close == nil || p.close.Started() {
		return ErrClosed
	}

	return p.client.checkOpen()
}

func (p *planHandle) ancestorCloseResult(ctx context.Context) (bool, error) {
	if p == nil || p.client == nil || p.close == nil {
		return true, ErrClosed
	}

	if p.close.Started() {
		return true, p.close.Wait(ctx)
	}

	return p.client.closeResult(ctx)
}

func (p *planHandle) release(ctx context.Context) error {
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

func (p *planHandle) newSession(
	ctx context.Context,
	configured runtimeSessionOptions,
) (*sessionHandle, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(configured.parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.sessionClient.CreateSession(ctx, &wirev1.CreateSessionRequest{
		ConnectionId:      p.client.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: p.id},
		Parameters:        converted,
		OutputContentType: configured.outputContentType,
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
