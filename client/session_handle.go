package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// sessionHandle is the private Wire handle for one durable Unified API session.
type sessionHandle struct {
	client *Client
	plan   *Plan
	id     string
	close  *closeState
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

func (s *sessionHandle) run(ctx context.Context) (*Execution, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	response, err := s.client.executionClient.RunSession(ctx, &wirev1.RunSessionRequest{
		ConnectionId: s.client.connectionProto(),
		SessionId:    &wirev1.SessionId{Value: s.id},
	})
	if err != nil {
		return nil, allocationRPCError(err)
	}

	return newExecutionHandle(s.client, s.plan, s, response.GetExecution())
}

func (s *sessionHandle) Close(ctx context.Context) error {
	if s == nil || s.client == nil || s.plan == nil || s.id == "" || s.close == nil {
		return ErrClosed
	}

	if s.close.Begin() {
		go settleHandleClose(ctx, "session", s.close, s.release)
	}

	return s.close.Wait(ctx)
}

func (s *sessionHandle) checkOpen() error {
	if s == nil || s.client == nil || s.plan == nil || s.id == "" || s.close == nil || s.close.Started() {
		return ErrClosed
	}

	return s.plan.checkOpen()
}

func (s *sessionHandle) ancestorCloseResult(ctx context.Context) (bool, error) {
	if s == nil || s.plan == nil || s.close == nil {
		return true, ErrClosed
	}

	if s.close.Started() {
		return true, s.close.Wait(ctx)
	}

	return s.plan.ancestorCloseResult(ctx)
}

func (s *sessionHandle) release(ctx context.Context) error {
	if closing, err := s.plan.ancestorCloseResult(ctx); closing {
		return err
	}

	_, err := s.client.sessionClient.ReleaseSession(ctx, &wirev1.ReleaseSessionRequest{
		ConnectionId: s.client.connectionProto(),
		SessionId:    &wirev1.SessionId{Value: s.id},
	})

	return decodeError(err)
}
