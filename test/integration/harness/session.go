package harness

import (
	"context"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/testoutput"
)

type (
	// SessionBehavior configures run and cleanup hooks for a durable hosted session.
	SessionBehavior struct {
		Run    func(context.Context, int) (*api.Content, error)
		Output func(context.Context, int) (api.Output, error)
		Close  func() error
	}

	// SessionSpy records repeated runs and cleanup of one hosted session.
	SessionSpy struct {
		id       int
		recorder *Recorder
		behavior SessionBehavior
	}
)

var _ api.Session = (*SessionSpy)(nil)

// Run records entry and settlement, passing the invocation count to the configured hook.
func (s *SessionSpy) Run(ctx context.Context) (api.Output, error) {
	call := s.recorder.record(Call{Resource: s.id, Method: "Run"})
	defer s.recorder.record(Call{Resource: s.id, Method: "RunFinished"})

	if s.behavior.Output != nil {
		return s.behavior.Output(ctx, call)
	}

	if s.behavior.Run != nil {
		content, err := s.behavior.Run(ctx, call)

		return testoutput.New(ctx, content, err), nil
	}

	return testoutput.New(ctx, &api.Content{}, nil), nil
}

// Close records entry and settlement around the configured cleanup hook.
func (s *SessionSpy) Close() error {
	s.recorder.record(Call{Resource: s.id, Method: "Close"})
	defer s.recorder.record(Call{Resource: s.id, Method: "CloseFinished"})

	if s.behavior.Close != nil {
		return s.behavior.Close()
	}

	return nil
}
