package core

import "sync"

// ConnectionRegistry owns the global logical-connection index and capacity.
type ConnectionRegistry struct {
	mu      sync.RWMutex
	max     int
	active  map[ConnectionID]*Connection
	closing map[ConnectionID]*Connection
	closed  bool
}

func NewConnectionRegistry(maxConnections int) *ConnectionRegistry {
	return &ConnectionRegistry{
		max:     maxConnections,
		active:  make(map[ConnectionID]*Connection),
		closing: make(map[ConnectionID]*Connection),
	}
}

func (r *ConnectionRegistry) Register(connection *Connection) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if connection == nil {
		return invalidRequest("connection is required")
	}

	if r.closed {
		return invalidState("server is shutting down", nil)
	}

	if r.active[connection.ID()] != nil || r.closing[connection.ID()] != nil {
		return invalidState("connection is already registered", nil)
	}

	if len(r.active)+len(r.closing) >= r.max {
		return resourceExhausted("logical connection limit reached")
	}

	r.active[connection.ID()] = connection

	return nil
}

func (r *ConnectionRegistry) Get(id ConnectionID) (*Connection, error) {
	if err := validateID(id, "connection ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	connection := r.active[id]
	r.mu.RUnlock()

	if connection == nil {
		return nil, notFound(ErrorKindConnectionNotFound, string(id))
	}

	return connection, nil
}

func (r *ConnectionRegistry) beginClose(id ConnectionID) (*Connection, bool, error) {
	if err := validateID(id, "connection ID"); err != nil {
		return nil, false, err
	}

	r.mu.Lock()
	connection := r.active[id]
	if connection != nil {
		delete(r.active, id)
		r.closing[id] = connection
	} else {
		connection = r.closing[id]
	}

	if connection == nil {
		r.mu.Unlock()

		return nil, false, notFound(ErrorKindConnectionNotFound, string(id))
	}

	started := connection.beginClose()
	r.mu.Unlock()

	return connection, started, nil
}

func (r *ConnectionRegistry) remove(id ConnectionID, expected *Connection) {
	r.mu.Lock()
	if r.closing[id] == expected {
		delete(r.closing, id)
	}
	r.mu.Unlock()
}

func (r *ConnectionRegistry) beginShutdown() []ConnectionID {
	r.mu.Lock()
	r.closed = true
	ids := make([]ConnectionID, 0, len(r.active)+len(r.closing))
	for id := range r.active {
		ids = append(ids, id)
	}

	for id := range r.closing {
		ids = append(ids, id)
	}
	r.mu.Unlock()

	return ids
}
