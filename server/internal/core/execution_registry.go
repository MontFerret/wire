package core

import "sync"

// ExecutionRegistry owns execution storage, ownership indexes, and accounting.
type ExecutionRegistry struct {
	mu          sync.RWMutex
	max         int
	maxWatchers int
	pending     map[ConnectionID]int
	active      map[ExecutionID]*Execution
	closing     map[ExecutionID]*Execution
	byOwner     map[ConnectionID]map[ExecutionID]*Execution
	byPlan      map[PlanID]map[ExecutionID]*Execution
	bySession   map[SessionID]map[ExecutionID]*Execution
}

func NewExecutionRegistry(maxExecutionsPerConnection, maxWatchers int) *ExecutionRegistry {
	return &ExecutionRegistry{
		max:         maxExecutionsPerConnection,
		maxWatchers: maxWatchers,
		pending:     make(map[ConnectionID]int),
		active:      make(map[ExecutionID]*Execution),
		closing:     make(map[ExecutionID]*Execution),
		byOwner:     make(map[ConnectionID]map[ExecutionID]*Execution),
		byPlan:      make(map[PlanID]map[ExecutionID]*Execution),
		bySession:   make(map[SessionID]map[ExecutionID]*Execution),
	}
}

func (r *ExecutionRegistry) reserve(owner ConnectionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending[owner]+len(r.byOwner[owner]) >= r.max {
		return resourceExhausted("execution limit reached")
	}

	r.pending[owner]++

	return nil
}

func (r *ExecutionRegistry) rollback(owner ConnectionID) {
	r.mu.Lock()
	r.pending[owner]--
	if r.pending[owner] == 0 {
		delete(r.pending, owner)
	}
	r.mu.Unlock()
}

func (r *ExecutionRegistry) commit(execution *Execution) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pending[execution.owner]--
	if r.pending[execution.owner] == 0 {
		delete(r.pending, execution.owner)
	}

	if r.active[execution.id] != nil || r.closing[execution.id] != nil {
		return invalidState("execution ID is already registered", nil)
	}

	r.active[execution.id] = execution
	owned := r.byOwner[execution.owner]
	if owned == nil {
		owned = make(map[ExecutionID]*Execution)
		r.byOwner[execution.owner] = owned
	}

	owned[execution.id] = execution
	if execution.planID != "" {
		children := r.byPlan[execution.planID]
		if children == nil {
			children = make(map[ExecutionID]*Execution)
			r.byPlan[execution.planID] = children
		}

		children[execution.id] = execution
	}

	if execution.sessionID != "" {
		children := r.bySession[execution.sessionID]
		if children == nil {
			children = make(map[ExecutionID]*Execution)
			r.bySession[execution.sessionID] = children
		}

		children[execution.id] = execution
	}

	return nil
}

func (r *ExecutionRegistry) get(owner ConnectionID, id ExecutionID) (*Execution, error) {
	if err := validateID(id, "execution ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	execution := r.active[id]
	if execution != nil && execution.owner != owner {
		execution = nil
	}
	r.mu.RUnlock()

	if execution == nil {
		return nil, notFound(ErrorKindExecutionNotFound, string(id))
	}

	return execution, nil
}

func (r *ExecutionRegistry) beginClose(owner ConnectionID, id ExecutionID) (*Execution, bool, error) {
	if err := validateID(id, "execution ID"); err != nil {
		return nil, false, err
	}

	r.mu.Lock()
	execution := r.active[id]
	started := false
	if execution != nil && execution.owner == owner {
		delete(r.active, id)
		r.closing[id] = execution
		started = execution.release.Begin()
	} else {
		execution = r.closing[id]
		if execution != nil && execution.owner != owner {
			execution = nil
		}
	}
	r.mu.Unlock()

	if execution == nil {
		return nil, false, notFound(ErrorKindExecutionNotFound, string(id))
	}

	return execution, started, nil
}

func (r *ExecutionRegistry) remove(execution *Execution) {
	r.mu.Lock()
	if r.closing[execution.id] == execution {
		delete(r.closing, execution.id)
		delete(r.byOwner[execution.owner], execution.id)

		if len(r.byOwner[execution.owner]) == 0 {
			delete(r.byOwner, execution.owner)
		}

		if execution.planID != "" {
			delete(r.byPlan[execution.planID], execution.id)

			if len(r.byPlan[execution.planID]) == 0 {
				delete(r.byPlan, execution.planID)
			}
		}

		if execution.sessionID != "" {
			delete(r.bySession[execution.sessionID], execution.id)

			if len(r.bySession[execution.sessionID]) == 0 {
				delete(r.bySession, execution.sessionID)
			}
		}
	}
	r.mu.Unlock()
}

func (r *ExecutionRegistry) listByOwner(owner ConnectionID) []ExecutionID {
	r.mu.RLock()
	ids := make([]ExecutionID, 0, len(r.byOwner[owner]))
	for id := range r.byOwner[owner] {
		ids = append(ids, id)
	}
	r.mu.RUnlock()

	return ids
}

func (r *ExecutionRegistry) listByPlan(owner ConnectionID, planID PlanID) []ExecutionID {
	r.mu.RLock()
	ids := make([]ExecutionID, 0, len(r.byPlan[planID]))
	for id, execution := range r.byPlan[planID] {
		if execution.owner == owner {
			ids = append(ids, id)
		}
	}
	r.mu.RUnlock()

	return ids
}

func (r *ExecutionRegistry) listBySession(owner ConnectionID, sessionID SessionID) []ExecutionID {
	r.mu.RLock()
	ids := make([]ExecutionID, 0, len(r.bySession[sessionID]))
	for id, execution := range r.bySession[sessionID] {
		if execution.owner == owner {
			ids = append(ids, id)
		}
	}
	r.mu.RUnlock()

	return ids
}
