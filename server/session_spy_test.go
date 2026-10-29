package server_test

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/testoutput"
)

type apiSessionSpy struct {
	mu         sync.Mutex
	run        func(context.Context) (*api.Content, error)
	close      func() error
	closeCalls int
}

func (s *apiSessionSpy) Run(ctx context.Context) (api.Output, error) {
	content, terminalErr := func() (*api.Content, error) {
		if s.run == nil {
			return &api.Content{}, nil
		}

		return s.run(ctx)
	}()

	return testoutput.New(ctx, content, terminalErr), nil
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
