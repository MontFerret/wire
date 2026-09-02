package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/google/uuid"
)

// debugSessionStore is the connection-wide index for plan-owned debug
// sessions. Plan membership remains authoritative for cascading release.
type debugSessionStore struct {
	mu             sync.RWMutex
	connection     *Connection
	max            int
	maxWatchers    int
	maxBreakpoints int
	pending        int
	active         map[DebugSessionID]*DebugSession
	closing        map[DebugSessionID]*DebugSession
}

func newDebugSessionStore(
	connection *Connection,
	maxSessions int,
	maxWatchers int,
	maxBreakpoints int,
) *debugSessionStore {
	return &debugSessionStore{
		connection:     connection,
		max:            maxSessions,
		maxWatchers:    maxWatchers,
		maxBreakpoints: maxBreakpoints,
		active:         make(map[DebugSessionID]*DebugSession),
		closing:        make(map[DebugSessionID]*DebugSession),
	}
}

func (c *Connection) OpenDebugSession(ctx context.Context, input OpenDebugInput) (DebugSnapshot, error) {
	return c.debugSessions.create(ctx, input)
}

func (c *Connection) ReleaseDebugSession(ctx context.Context, id DebugSessionID) error {
	return c.debugSessions.release(ctx, id)
}

func (s *debugSessionStore) create(ctx context.Context, input OpenDebugInput) (DebugSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return DebugSnapshot{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return DebugSnapshot{}, err
	}

	if err := s.connection.beginOperation(); err != nil {
		return DebugSnapshot{}, err
	}
	defer s.connection.finishOperation()

	if err := s.reserveCreation(); err != nil {
		return DebugSnapshot{}, err
	}
	committed := false
	defer func() {
		if !committed {
			s.rollbackCreation()
		}
	}()

	plan, err := s.connection.plans.beginDebugCreation(input.PlanID)
	if err != nil {
		return DebugSnapshot{}, err
	}
	defer plan.debugCreations.Done()

	options := []api.SessionOption{api.WithParams(cloneParameters(input.Parameters))}
	if input.OutputContentType != "" {
		options = append(options, api.WithOutputContentType(input.OutputContentType))
	}

	openCtx, cancelOpen := s.connection.operationContext(ctx)
	defer cancelOpen()

	runtimeDebugger, err := plan.plan.NewDebugSession(openCtx, options...)
	if err != nil {
		if !isNil(runtimeDebugger) {
			return DebugSnapshot{}, errors.Join(internalError(err), closeAPIDebugSession(runtimeDebugger))
		}

		return DebugSnapshot{}, internalError(err)
	}

	if isNil(runtimeDebugger) {
		return DebugSnapshot{}, internalError(errors.New("runtime returned no debug session"))
	}

	if err := openCtx.Err(); err != nil {
		return DebugSnapshot{}, errors.Join(err, closeAPIDebugSession(runtimeDebugger))
	}

	debugCtx, cancel := context.WithCancelCause(s.connection.ctx)
	created := &DebugSession{
		id:             DebugSessionID(uuid.NewString()),
		plan:           plan,
		debugger:       runtimeDebugger,
		ctx:            debugCtx,
		cancel:         cancel,
		state:          DebugCreated,
		breakpoints:    make(map[debugger.BreakpointID]debugger.Breakpoint),
		maxWatchers:    s.maxWatchers,
		maxBreakpoints: s.maxBreakpoints,
		watchers:       make(map[uint64]*debugWatcher),
	}

	err = s.connection.commitCreation(func() error {
		return s.connection.plans.withActive(input.PlanID, plan, func(current *Plan) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			s.mu.Lock()
			s.pending--
			s.active[created.id] = created
			current.debugSessions[created.id] = struct{}{}
			s.mu.Unlock()
			committed = true

			return nil
		})
	})
	if err != nil {
		return DebugSnapshot{}, errors.Join(err, closeAPIDebugSession(runtimeDebugger))
	}

	return created.snapshot(), nil
}

func (s *debugSessionStore) release(ctx context.Context, id DebugSessionID) error {
	if err := validateID(id, "debug session ID"); err != nil {
		return err
	}

	s.mu.Lock()
	session := s.active[id]
	if session != nil {
		delete(s.active, id)
		s.closing[id] = session
	} else {
		session = s.closing[id]
	}
	s.mu.Unlock()

	if session == nil {
		return notFound(ErrorDebugSessionNotFound, string(id))
	}

	if session.release.Begin() {
		go s.settleRelease(session)
	}

	return session.release.Wait(ctx)
}

func (s *debugSessionStore) settleRelease(session *DebugSession) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("debug session release panicked")))
		}

		session.plan.mu.Lock()
		delete(session.plan.debugSessions, session.id)
		session.plan.mu.Unlock()

		s.mu.Lock()
		if s.closing[session.id] == session {
			delete(s.closing, session.id)
		}
		s.mu.Unlock()

		session.release.Finish(err)
	}()

	err = session.Close(context.Background())
}

func (s *debugSessionStore) lookup(id DebugSessionID) (*DebugSession, error) {
	if err := validateID(id, "debug session ID"); err != nil {
		return nil, err
	}

	s.mu.RLock()
	session := s.active[id]
	s.mu.RUnlock()

	if session == nil {
		return nil, notFound(ErrorDebugSessionNotFound, string(id))
	}

	return session, nil
}

func (s *debugSessionStore) reserveCreation() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending+len(s.active)+len(s.closing) >= s.max {
		return resourceExhausted("debug session limit reached")
	}

	s.pending++

	return nil
}

func (s *debugSessionStore) rollbackCreation() {
	s.mu.Lock()
	s.pending--
	s.mu.Unlock()
}

func (s *debugSessionStore) closeAll() error {
	s.mu.RLock()
	ids := make([]DebugSessionID, 0, len(s.active)+len(s.closing))
	for id := range s.active {
		ids = append(ids, id)
	}

	for id := range s.closing {
		if s.active[id] == nil {
			ids = append(ids, id)
		}
	}
	s.mu.RUnlock()

	var result error
	for _, id := range ids {
		err := s.release(context.Background(), id)
		result = errors.Join(result, ignoreMissingResource(err, ErrorDebugSessionNotFound))
	}

	return result
}
