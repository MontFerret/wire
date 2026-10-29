package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/testoutput"
)

type spySession struct {
	mu         sync.Mutex
	run        func(context.Context) (*api.Content, error)
	close      func() error
	runCalls   int
	closeCalls int
}

func (s *spySession) Run(ctx context.Context) (api.Output, error) {
	content, terminalErr := func() (*api.Content, error) {
		s.mu.Lock()
		s.runCalls++
		run := s.run
		s.mu.Unlock()

		if run == nil {
			return &api.Content{}, nil
		}

		return run(ctx)
	}()

	return testoutput.New(ctx, content, terminalErr), nil
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
