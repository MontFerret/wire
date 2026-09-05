package core

type testExecutionRegistry struct {
	registry *ExecutionRegistry
	owner    ConnectionID
}

func (r testExecutionRegistry) lookup(id ExecutionID) (*Execution, error) {
	return r.registry.get(r.owner, id)
}
