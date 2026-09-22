package client

import (
	"context"

	"github.com/MontFerret/api"
)

type remoteSession struct {
	session *sessionHandle
}

var _ api.Session = (*remoteSession)(nil)

func (s *remoteSession) Run(ctx context.Context) (*api.Output, error) {
	if s == nil || s.session == nil {
		return nil, ErrClosed
	}

	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	creationCtx, cancel, err := runtimeAllocationContext(ctx)
	if err != nil {
		return nil, err
	}

	execution, err := s.session.run(creationCtx)
	cancel()

	if err != nil {
		return nil, s.session.client.reclaimAllocation(ctx, err, s.session.Close, s.session.plan.Close)
	}

	return execution.waitAndRelease(ctx)
}

func (s *remoteSession) Close() error {
	if s == nil || s.session == nil {
		return ErrClosed
	}

	return boundedCleanup(context.Background(), convenienceCleanupTimeout, s.session.Close)
}
