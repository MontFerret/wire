package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
)

type spySession struct {
	mu         sync.Mutex
	run        func(context.Context) (api.Output, error)
	close      func() error
	runCalls   int
	closeCalls int
}

func (s *spySession) Run(ctx context.Context) (api.Output, error) {
	s.mu.Lock()
	s.runCalls++
	run := s.run
	s.mu.Unlock()

	if run == nil {
		return api.Output{}, nil
	}

	return run(ctx)
}

func (s *spySession) Close() error {
	s.mu.Lock()
	s.closeCalls++
	closeSession := s.close
	s.mu.Unlock()

	if closeSession == nil {
		return nil
	}

	return closeSession()
}

func (s *spySession) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.runCalls, s.closeCalls
}
