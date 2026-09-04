package core

import "sync"

// DebugSessionRegistry owns debugger-session storage, ownership indexes, and
// per-connection accounting.
type DebugSessionRegistry struct {
	mu             sync.RWMutex
	max            int
	maxWatchers    int
	maxBreakpoints int
	pending        map[ConnectionID]int
	active         map[DebugSessionID]*DebugSession
	closing        map[DebugSessionID]*DebugSession
	byOwner        map[ConnectionID]map[DebugSessionID]*DebugSession
	byPlan         map[PlanID]map[DebugSessionID]*DebugSession
}

func NewDebugSessionRegistry(maxSessionsPerConnection, maxWatchers, maxBreakpoints int) *DebugSessionRegistry {
	return &DebugSessionRegistry{
		max:            maxSessionsPerConnection,
		maxWatchers:    maxWatchers,
		maxBreakpoints: maxBreakpoints,
		pending:        make(map[ConnectionID]int),
		active:         make(map[DebugSessionID]*DebugSession),
		closing:        make(map[DebugSessionID]*DebugSession),
		byOwner:        make(map[ConnectionID]map[DebugSessionID]*DebugSession),
		byPlan:         make(map[PlanID]map[DebugSessionID]*DebugSession),
	}
}

func (r *DebugSessionRegistry) reserve(owner ConnectionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending[owner]+len(r.byOwner[owner]) >= r.max {
		return resourceExhausted("debug session limit reached")
	}

	r.pending[owner]++

	return nil
}

func (r *DebugSessionRegistry) rollback(owner ConnectionID) {
	r.mu.Lock()
	r.pending[owner]--
	if r.pending[owner] == 0 {
		delete(r.pending, owner)
	}
	r.mu.Unlock()
}

func (r *DebugSessionRegistry) commit(session *DebugSession) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pending[session.owner]--
	if r.pending[session.owner] == 0 {
		delete(r.pending, session.owner)
	}

	if r.active[session.id] != nil || r.closing[session.id] != nil {
		return invalidState("debug session ID is already registered", nil)
	}

	r.active[session.id] = session
	owned := r.byOwner[session.owner]
	if owned == nil {
		owned = make(map[DebugSessionID]*DebugSession)
		r.byOwner[session.owner] = owned
	}

	owned[session.id] = session
	children := r.byPlan[session.planID]
	if children == nil {
		children = make(map[DebugSessionID]*DebugSession)
		r.byPlan[session.planID] = children
	}

	children[session.id] = session

	return nil
}

func (r *DebugSessionRegistry) get(owner ConnectionID, id DebugSessionID) (*DebugSession, error) {
	if err := validateID(id, "debug session ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	session := r.active[id]
	if session != nil && session.owner != owner {
		session = nil
	}
	r.mu.RUnlock()

	if session == nil {
		return nil, notFound(ErrorKindDebugSessionNotFound, string(id))
	}

	return session, nil
}

func (r *DebugSessionRegistry) beginClose(owner ConnectionID, id DebugSessionID) (*DebugSession, bool, error) {
	if err := validateID(id, "debug session ID"); err != nil {
		return nil, false, err
	}

	r.mu.Lock()
	session := r.active[id]
	started := false
	if session != nil && session.owner == owner {
		delete(r.active, id)
		r.closing[id] = session
		started = session.release.Begin()
	} else {
		session = r.closing[id]
		if session != nil && session.owner != owner {
			session = nil
		}
	}
	r.mu.Unlock()

	if session == nil {
		return nil, false, notFound(ErrorKindDebugSessionNotFound, string(id))
	}

	return session, started, nil
}

func (r *DebugSessionRegistry) remove(session *DebugSession) {
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

func (r *DebugSessionRegistry) listByOwner(owner ConnectionID) []DebugSessionID {
	r.mu.RLock()
	ids := make([]DebugSessionID, 0, len(r.byOwner[owner]))
	for id := range r.byOwner[owner] {
		ids = append(ids, id)
	}
	r.mu.RUnlock()

	return ids
}

func (r *DebugSessionRegistry) listByPlan(owner ConnectionID, planID PlanID) []DebugSessionID {
	r.mu.RLock()
	ids := make([]DebugSessionID, 0, len(r.byPlan[planID]))
	for id, session := range r.byPlan[planID] {
		if session.owner == owner {
			ids = append(ids, id)
		}
	}
	r.mu.RUnlock()

	return ids
}
