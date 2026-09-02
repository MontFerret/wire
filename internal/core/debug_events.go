package core

func (c *Connection) WatchDebug(id DebugSessionID) (DebugSubscription, error) {
	return c.debugSessions.watch(id)
}

func (s *debugSessionStore) watch(id DebugSessionID) (DebugSubscription, error) {
	session, err := s.lookup(id)
	if err != nil {
		return DebugSubscription{}, err
	}

	return session.subscribe()
}
