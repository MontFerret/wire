package core

type testPlanRegistry struct {
	registry *PlanRegistry
	owner    ConnectionID
}

func (r testPlanRegistry) lookup(id PlanID) (*Plan, error) {
	return r.registry.get(r.owner, id)
}
