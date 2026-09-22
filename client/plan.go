package client

import (
	"context"
	"errors"
	"sync"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// planHandle is a compiled remote runtime plan owned by one connectionHandle.
type planHandle struct {
	client      *connectionHandle
	id          string
	parameters  []string
	close       *closeState
	owner       *remoteRuntime
	lifecycleMu sync.Mutex
	creating    sync.WaitGroup
	children    int
	apiClose    closeState
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
func (p *planHandle) NewDebugSession(ctx context.Context, configured runtimeSessionOptions) (result *debugSessionHandle, resultErr error) {
	if err := p.beginChild(); err != nil {
		return nil, err
	}

	defer func() {
		p.creating.Done()

		if result == nil {
			resultErr = errors.Join(resultErr, p.childDone(ctx))
		}
	}()

	if err := p.checkTransportOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(configured.parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.debugClient.CreateDebugSession(ctx, &wirev1.CreateDebugSessionRequest{
		ConnectionId:         p.client.connectionProto(),
		PlanId:               &wirev1.PlanId{Value: p.id},
		Parameters:           converted,
		OutputContentType:    configured.outputContentType,
		OutputContentTypeSet: configured.outputContentTypeSet,
		FsRoot:               configured.fsRoot,
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

func (p *planHandle) checkTransportOpen() error {
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

func (p *planHandle) release(ctx context.Context) (resultErr error) {
	defer func() {
		if p.owner != nil {
			resultErr = errors.Join(resultErr, p.owner.drop())
		}
	}()

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
) (result *sessionHandle, resultErr error) {
	if err := p.beginChild(); err != nil {
		return nil, err
	}

	defer func() {
		p.creating.Done()

		if result == nil {
			resultErr = errors.Join(resultErr, p.childDone(ctx))
		}
	}()

	if err := p.checkTransportOpen(); err != nil {
		return nil, err
	}

	converted, err := encodeParameters(configured.parameters)
	if err != nil {
		return nil, err
	}

	response, err := p.client.sessionClient.CreateSession(ctx, &wirev1.CreateSessionRequest{
		ConnectionId:         p.client.connectionProto(),
		PlanId:               &wirev1.PlanId{Value: p.id},
		Parameters:           converted,
		OutputContentType:    configured.outputContentType,
		OutputContentTypeSet: configured.outputContentTypeSet,
		FsRoot:               configured.fsRoot,
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

func (p *planHandle) beginChild() error {
	if p == nil {
		return ErrClosed
	}

	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if p.apiClose.Started() {
		return ErrClosed
	}

	if err := p.checkTransportOpen(); err != nil {
		return err
	}

	p.children++
	p.creating.Add(1)

	return nil
}

func (p *planHandle) childDone(ctx context.Context) error {
	p.lifecycleMu.Lock()
	p.children--
	release := p.children == 0 && p.apiClose.Started()
	p.lifecycleMu.Unlock()

	if !release {
		return nil
	}

	// ClosePlan must settle before the transport handle can be released.
	_ = p.apiClose.Wait(context.Background())

	return p.Close(ctx)
}

func (p *planHandle) closeAPI(ctx context.Context) error {
	p.lifecycleMu.Lock()
	started := p.apiClose.Begin()
	p.lifecycleMu.Unlock()

	if started {
		go settleHandleClose(ctx, "plan API", &p.apiClose, p.settleAPIClose)
	}

	return p.apiClose.Wait(ctx)
}

func (p *planHandle) settleAPIClose(ctx context.Context) error {
	p.creating.Wait()

	if closing, err := p.ancestorCloseResult(ctx); closing {
		return err
	}

	_, err := p.client.planClient.ClosePlan(ctx, &wirev1.ClosePlanRequest{
		ConnectionId: p.client.connectionProto(), PlanId: &wirev1.PlanId{Value: p.id},
	})
	result := decodeError(err)
	p.lifecycleMu.Lock()
	release := p.children == 0
	p.lifecycleMu.Unlock()

	if release {
		result = errors.Join(result, p.Close(ctx))
	}

	return result
}
