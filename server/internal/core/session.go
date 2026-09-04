package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/lifecycle"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

type (
	// Session owns one durable Unified API session and admits one execution at a time.
	Session struct {
		mu             sync.Mutex
		id             SessionID
		owner          ConnectionID
		planID         PlanID
		session        api.Session
		ctx            context.Context
		cancel         context.CancelCauseFunc
		closing        bool
		poisoned       bool
		active         ExecutionID
		childCreations sync.WaitGroup
		close          lifecycle.Close
		release        lifecycle.Close
	}

	CreateSessionInput struct {
		PlanID            PlanID
		Parameters        map[string]any
		OutputContentType string
	}

	SessionSnapshot struct {
		ID SessionID
	}
)

func newSession(
	id SessionID,
	owner ConnectionID,
	planID PlanID,
	session api.Session,
	ctx context.Context,
	cancel context.CancelCauseFunc,
) *Session {
	return &Session{
		id:      id,
		owner:   owner,
		planID:  planID,
		session: session,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *Session) ID() SessionID {
	return s.id
}

func (s *Session) snapshot() SessionSnapshot {
	return SessionSnapshot{ID: s.id}
}

func (s *Session) Context() context.Context {
	return s.ctx
}

func (s *Session) Run(ctx context.Context) (api.Output, error) {
	output, err := panicboundary.Call(func() (api.Output, error) {
		return s.session.Run(ctx)
	})
	var panicErr *panicboundary.Error
	if errors.As(err, &panicErr) {
		s.mu.Lock()
		s.poisoned = true
		s.mu.Unlock()
	}

	return output, runtimePanicError("run runtime session", err)
}

func (s *Session) beginExecution(id ExecutionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return notFound(ErrorKindSessionNotFound, string(s.id))
	}
	if s.poisoned {
		return invalidState("session cannot run after a runtime panic", nil)
	}

	if s.active != "" {
		return invalidState("session already has an active execution", nil)
	}

	s.active = id
	s.childCreations.Add(1)

	return nil
}

func (s *Session) finishExecutionCreation() {
	s.childCreations.Done()
}

func (s *Session) finishExecution(id ExecutionID) {
	s.mu.Lock()
	if s.active == id {
		s.active = ""
	}
	s.mu.Unlock()
}

func (s *Session) markClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return false
	}

	s.closing = true
	s.cancel(context.Canceled)

	return s.release.Begin()
}

func (s *Session) waitChildCreations() {
	s.childCreations.Wait()
}

func (s *Session) Close(ctx context.Context) error {
	if s.close.Begin() {
		go s.settleClose()
	}

	return s.close.Wait(ctx)
}

func (s *Session) settleClose() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("session cleanup panicked")))
		}

		s.close.Finish(err)
	}()

	s.cancel(context.Canceled)
	err = closeAPISession(s.session)
}

func (s *Session) finishRelease(err error) {
	s.release.Finish(err)
}

func (s *Session) waitRelease(ctx context.Context) error {
	return s.release.Wait(ctx)
}
