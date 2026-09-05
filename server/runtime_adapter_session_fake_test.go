package server_test

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
)

type contractSession struct {
	mu         sync.Mutex
	run        func(context.Context, int) (api.Output, error)
	runCalls   int
	closeCalls int
}

func (s *contractSession) Run(ctx context.Context) (api.Output, error) {
	s.mu.Lock()
	s.runCalls++
	call := s.runCalls
	run := s.run
	s.mu.Unlock()
	if run == nil {
		return api.Output{}, nil
	}

	return run(ctx, call)
}

func (s *contractSession) Close() error {
	s.mu.Lock()
	s.closeCalls++
	s.mu.Unlock()

	return nil
}

func (s *contractSession) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.runCalls, s.closeCalls
}
