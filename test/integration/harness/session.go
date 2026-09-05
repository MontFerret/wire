package harness

import (
	"context"

	"github.com/MontFerret/api"
)

type (
	SessionBehavior struct {
		Run   func(context.Context, int) (api.Output, error)
		Close func() error
	}

	SessionSpy struct {
		id       int
		recorder *Recorder
		behavior SessionBehavior
	}
)

var _ api.Session = (*SessionSpy)(nil)

func (s *SessionSpy) Run(ctx context.Context) (api.Output, error) {
	call := s.recorder.record(Call{Resource: s.id, Method: "Run"})
	defer s.recorder.record(Call{Resource: s.id, Method: "RunFinished"})

	if s.behavior.Run != nil {
		return s.behavior.Run(ctx, call)
	}

	return api.Output{}, nil
}

func (s *SessionSpy) Close() error {
	s.recorder.record(Call{Resource: s.id, Method: "Close"})
	defer s.recorder.record(Call{Resource: s.id, Method: "CloseFinished"})

	if s.behavior.Close != nil {
		return s.behavior.Close()
	}

	return nil
}
