package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/lifecycle"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

// Session owns one durable hosted session. Its execution slot remains occupied
// through execution release, including after the run reaches a terminal state.
type Session struct {
	id       SessionID
	plan     *Plan
	session  api.Session
	ctx      context.Context
	cancel   context.CancelCauseFunc
	poisoned atomic.Bool
	// active and creation admission are guarded by plan.store.mu.
	active   *Execution
	creating sync.WaitGroup
	release  lifecycle.Close
}

func newSession(plan *Plan, hosted api.Session) *Session {
	ctx, cancel := context.WithCancelCause(plan.store.ctx)

	return &Session{
		id:      SessionID(uuid.NewString()),
		plan:    plan,
		session: hosted,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// ID identifies this durable session within its logical connection.
func (s *Session) ID() SessionID {
	return s.id
}

// Execute starts a run only when the session's previous execution has been released.
func (s *Session) Execute(ctx context.Context) (*Execution, error) {
	if err := s.plan.store.operationError(ctx); err != nil {
		return nil, err
	}

	r := s.plan.store
	if err := r.beginCreation(executionResource, s.plan); err != nil {
		return nil, err
	}

	committed := false
	defer func() { r.finishCreation(executionResource, s.plan, committed) }()

	r.mu.Lock()
	if r.sessions[s.id] != s || s.release.Started() {
		r.mu.Unlock()

		return nil, notFound(ErrorKindSessionNotFound, string(s.id))
	}

	if s.poisoned.Load() {
		r.mu.Unlock()

		return nil, invalidState("session cannot run after a runtime panic", nil)
	}

	if s.active != nil {
		r.mu.Unlock()

		return nil, invalidState("session already has an active execution", nil)
	}

	created := newExecution(r, s.plan, s, s.run, nil)
	s.active = created
	s.creating.Add(1)
	r.mu.Unlock()
	defer s.creating.Done()

	if err := r.registerExecution(ctx, created); err != nil {
		created.cancel(context.Canceled)
		r.mu.Lock()
		s.active = nil
		r.mu.Unlock()

		return nil, err
	}

	committed = true
	go created.run()

	return created, nil
}

func (s *Session) run(ctx context.Context) (api.Output, error) {
	output, err := panicboundary.Call(func() (api.Output, error) {
		return s.session.Run(ctx)
	})

	var panicErr *panicboundary.Error
	if errors.As(err, &panicErr) {
		s.poisoned.Store(true)
	}

	return output, runtimePanicError("run runtime session", err)
}

// Release cancels the session, releases its execution, and closes the hosted session.
// Caller cancellation stops waiting without abandoning teardown.
func (s *Session) Release(ctx context.Context) error {
	r := s.plan.store
	r.mu.Lock()

	started := s.release.Begin()
	if started {
		s.cancel(context.Canceled)
	}

	r.mu.Unlock()

	if started {
		go s.settleRelease()
	}

	return s.release.Wait(ctx)
}

func (s *Session) settleRelease() {
	var err error
	r := s.plan.store
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("session release panicked")))
		}

		r.removeSession(s)

		s.release.Finish(err)
	}()

	s.creating.Wait()
	r.mu.Lock()
	execution := s.active
	r.mu.Unlock()

	if execution != nil {
		err = execution.Release(context.Background())
	}

	err = errors.Join(err, closeAPISession(s.session))
}
