package server_test

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
)

type apiSessionSpy struct {
	mu         sync.Mutex
	run        func(context.Context) (api.Output, error)
	close      func() error
	closeCalls int
}

func (s *apiSessionSpy) Run(ctx context.Context) (api.Output, error) {
	if s.run == nil {
		return api.Output{}, nil
	}

	return s.run(ctx)
}

func (s *apiSessionSpy) Close() error {
	s.mu.Lock()
	s.closeCalls++
	closeSession := s.close
	s.mu.Unlock()

	if closeSession == nil {
		return nil
	}

	return closeSession()
}
