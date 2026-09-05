package core

import "sync"

// SessionRegistry owns durable execution-session storage, ownership indexes,
// and per-connection accounting.
type SessionRegistry struct {
	mu      sync.RWMutex
	max     int
	pending map[ConnectionID]int
	active  map[SessionID]*Session
	closing map[SessionID]*Session
	byOwner map[ConnectionID]map[SessionID]*Session
	byPlan  map[PlanID]map[SessionID]*Session
}

func NewSessionRegistry(maxSessionsPerConnection int) *SessionRegistry {
	return &SessionRegistry{
		max:     maxSessionsPerConnection,
		pending: make(map[ConnectionID]int),
		active:  make(map[SessionID]*Session),
		closing: make(map[SessionID]*Session),
		byOwner: make(map[ConnectionID]map[SessionID]*Session),
		byPlan:  make(map[PlanID]map[SessionID]*Session),
	}
}

func (r *SessionRegistry) reserve(owner ConnectionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending[owner]+len(r.byOwner[owner]) >= r.max {
		return resourceExhausted("session limit reached")
	}

	r.pending[owner]++

	return nil
}

func (r *SessionRegistry) rollback(owner ConnectionID) {
	r.mu.Lock()
	r.pending[owner]--
	if r.pending[owner] == 0 {
		delete(r.pending, owner)
	}
	r.mu.Unlock()
}

func (r *SessionRegistry) commit(session *Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pending[session.owner]--
	if r.pending[session.owner] == 0 {
		delete(r.pending, session.owner)
	}

	if r.active[session.id] != nil || r.closing[session.id] != nil {
		return invalidState("session ID is already registered", nil)
	}

	r.active[session.id] = session
	owned := r.byOwner[session.owner]
	if owned == nil {
		owned = make(map[SessionID]*Session)
		r.byOwner[session.owner] = owned
	}
	owned[session.id] = session

	children := r.byPlan[session.planID]
	if children == nil {
		children = make(map[SessionID]*Session)
		r.byPlan[session.planID] = children
	}
	children[session.id] = session

	return nil
}

func (r *SessionRegistry) get(owner ConnectionID, id SessionID) (*Session, error) {
	if err := validateID(id, "session ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	session := r.active[id]
	if session != nil && session.owner != owner {
		session = nil
	}
	r.mu.RUnlock()

	if session == nil {
		return nil, notFound(ErrorKindSessionNotFound, string(id))
	}

	return session, nil
}

func (r *SessionRegistry) beginExecution(owner ConnectionID, id SessionID, executionID ExecutionID) (*Session, error) {
	if err := validateID(id, "session ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	session := r.active[id]
	if session == nil || session.owner != owner {
		r.mu.RUnlock()

		return nil, notFound(ErrorKindSessionNotFound, string(id))
	}

	err := session.beginExecution(executionID)
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	return session, nil
}

func (r *SessionRegistry) commitExecution(
	owner ConnectionID,
	id SessionID,
	expected *Session,
	executionID ExecutionID,
	commit func() error,
) error {
	r.mu.RLock()
	session := r.active[id]
	if session == nil || session != expected || session.owner != owner {
		r.mu.RUnlock()

		return notFound(ErrorKindSessionNotFound, string(id))
	}

	session.mu.Lock()
	if session.closing || session.active != executionID {
		session.mu.Unlock()
		r.mu.RUnlock()

		return notFound(ErrorKindSessionNotFound, string(id))
	}

	err := commit()
	session.mu.Unlock()
	r.mu.RUnlock()

	return err
}

func (r *SessionRegistry) finishExecution(owner ConnectionID, id SessionID, executionID ExecutionID) {
	r.mu.RLock()
	session := r.active[id]
	if session == nil {
		session = r.closing[id]
	}
	if session != nil && session.owner == owner {
		session.finishExecution(executionID)
	}
	r.mu.RUnlock()
}

func (r *SessionRegistry) beginClose(owner ConnectionID, id SessionID) (*Session, bool, error) {
	if err := validateID(id, "session ID"); err != nil {
		return nil, false, err
	}

	r.mu.Lock()
	session := r.active[id]
	if session != nil && session.owner == owner {
		delete(r.active, id)
		r.closing[id] = session
	} else {
		session = r.closing[id]
		if session != nil && session.owner != owner {
			session = nil
		}
	}

	if session == nil {
		r.mu.Unlock()

		return nil, false, notFound(ErrorKindSessionNotFound, string(id))
	}

	started := session.markClosing()
	r.mu.Unlock()

	return session, started, nil
}

func (r *SessionRegistry) remove(session *Session) {
	r.mu.Lock()
	if r.closing[session.id] == session {
		delete(r.closing, session.id)
		delete(r.byOwner[session.owner], session.id)
		if len(r.byOwner[session.owner]) == 0 {
			delete(r.byOwner, session.owner)
		}

		delete(r.byPlan[session.planID], session.id)
		if len(r.byPlan[session.planID]) == 0 {
			delete(r.byPlan, session.planID)
		}
	}
	r.mu.Unlock()
}

func (r *SessionRegistry) listByOwner(owner ConnectionID) []SessionID {
	r.mu.RLock()
	ids := make([]SessionID, 0, len(r.byOwner[owner]))
	for id := range r.byOwner[owner] {
		ids = append(ids, id)
	}
	r.mu.RUnlock()

	return ids
}

func (r *SessionRegistry) listByPlan(owner ConnectionID, planID PlanID) []SessionID {
	r.mu.RLock()
	ids := make([]SessionID, 0, len(r.byPlan[planID]))
	for id, session := range r.byPlan[planID] {
		if session.owner == owner {
			ids = append(ids, id)
		}
	}
	r.mu.RUnlock()

	return ids
}
