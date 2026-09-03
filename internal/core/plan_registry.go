package core

import "sync"

// PlanRegistry owns the global plan index, explicit connection ownership, and
// per-connection plan accounting.
type PlanRegistry struct {
	mu      sync.RWMutex
	max     int
	pending map[ConnectionID]int
	active  map[PlanID]*Plan
	closing map[PlanID]*Plan
	byOwner map[ConnectionID]map[PlanID]*Plan
}

func NewPlanRegistry(maxPlansPerConnection int) *PlanRegistry {
	return &PlanRegistry{
		max:     maxPlansPerConnection,
		pending: make(map[ConnectionID]int),
		active:  make(map[PlanID]*Plan),
		closing: make(map[PlanID]*Plan),
		byOwner: make(map[ConnectionID]map[PlanID]*Plan),
	}
}

func (r *PlanRegistry) reserve(owner ConnectionID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending[owner]+len(r.byOwner[owner]) >= r.max {
		return resourceExhausted("plan limit reached")
	}

	r.pending[owner]++

	return nil
}

func (r *PlanRegistry) rollback(owner ConnectionID) {
	r.mu.Lock()
	r.pending[owner]--
	if r.pending[owner] == 0 {
		delete(r.pending, owner)
	}
	r.mu.Unlock()
}

// commit consumes one reservation regardless of whether publication succeeds.
func (r *PlanRegistry) commit(plan *Plan) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pending[plan.owner]--
	if r.pending[plan.owner] == 0 {
		delete(r.pending, plan.owner)
	}

	if r.active[plan.id] != nil || r.closing[plan.id] != nil {
		return invalidState("plan ID is already registered", nil)
	}

	r.active[plan.id] = plan
	owned := r.byOwner[plan.owner]
	if owned == nil {
		owned = make(map[PlanID]*Plan)
		r.byOwner[plan.owner] = owned
	}
	owned[plan.id] = plan

	return nil
}

func (r *PlanRegistry) get(owner ConnectionID, id PlanID) (*Plan, error) {
	if err := validateID(id, "plan ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	plan := r.active[id]
	if plan != nil && plan.owner != owner {
		plan = nil
	}
	r.mu.RUnlock()

	if plan == nil {
		return nil, notFound(ErrorPlanNotFound, string(id))
	}

	return plan, nil
}

func (r *PlanRegistry) beginChild(owner ConnectionID, id PlanID, debug bool) (*Plan, error) {
	if err := validateID(id, "plan ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	plan := r.active[id]
	if plan == nil || plan.owner != owner {
		r.mu.RUnlock()

		return nil, notFound(ErrorPlanNotFound, string(id))
	}

	err := plan.beginChildCreation(debug)
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	return plan, nil
}

// commitChild keeps active-plan validation atomic with child publication. The
// callback must not call external code.
func (r *PlanRegistry) commitChild(owner ConnectionID, id PlanID, expected *Plan, commit func() error) error {
	r.mu.RLock()
	plan := r.active[id]
	if plan == nil || plan != expected || plan.owner != owner {
		r.mu.RUnlock()

		return notFound(ErrorPlanNotFound, string(id))
	}

	plan.mu.Lock()
	if plan.closing {
		plan.mu.Unlock()
		r.mu.RUnlock()

		return notFound(ErrorPlanNotFound, string(id))
	}

	err := commit()
	plan.mu.Unlock()
	r.mu.RUnlock()

	return err
}

func (r *PlanRegistry) beginClose(owner ConnectionID, id PlanID) (*Plan, bool, error) {
	if err := validateID(id, "plan ID"); err != nil {
		return nil, false, err
	}

	r.mu.Lock()
	plan := r.active[id]
	if plan != nil && plan.owner == owner {
		delete(r.active, id)
		r.closing[id] = plan
	} else {
		plan = r.closing[id]
		if plan != nil && plan.owner != owner {
			plan = nil
		}
	}

	if plan == nil {
		r.mu.Unlock()

		return nil, false, notFound(ErrorPlanNotFound, string(id))
	}

	started := plan.markClosing()
	r.mu.Unlock()

	return plan, started, nil
}

func (r *PlanRegistry) remove(plan *Plan) {
	r.mu.Lock()
	if r.closing[plan.id] == plan {
		delete(r.closing, plan.id)
		delete(r.byOwner[plan.owner], plan.id)
		if len(r.byOwner[plan.owner]) == 0 {
			delete(r.byOwner, plan.owner)
		}
	}
	r.mu.Unlock()
}

func (r *PlanRegistry) listByOwner(owner ConnectionID) []PlanID {
	r.mu.RLock()
	ids := make([]PlanID, 0, len(r.byOwner[owner]))
	for id := range r.byOwner[owner] {
		ids = append(ids, id)
	}
	r.mu.RUnlock()

	return ids
}
