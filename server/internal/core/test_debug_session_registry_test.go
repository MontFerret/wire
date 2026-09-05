package core

type testDebugSessionRegistry struct {
	registry *DebugSessionRegistry
	owner    ConnectionID
}

func (r testDebugSessionRegistry) lookup(id DebugSessionID) (*DebugSession, error) {
	return r.registry.get(r.owner, id)
}
