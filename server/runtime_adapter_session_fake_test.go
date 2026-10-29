package server_test

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/testoutput"
)

type contractSession struct {
	mu         sync.Mutex
	run        func(context.Context, int) (*api.Content, error)
	runCalls   int
	closeCalls int
}

func (s *contractSession) Run(ctx context.Context) (api.Output, error) {
	content, terminalErr := func() (*api.Content, error) {
		s.mu.Lock()
		s.runCalls++
		call := s.runCalls
		run := s.run
		s.mu.Unlock()

		if run == nil {
			return &api.Content{}, nil
		}

		return run(ctx, call)
	}()

	return testoutput.New(ctx, content, terminalErr), nil
}

func (s *contractSession) Close() error {
	s.mu.Lock()
	s.closeCalls++
	s.mu.Unlock()

	return nil
}
