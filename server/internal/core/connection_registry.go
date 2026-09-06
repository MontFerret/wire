package core

import (
	"context"
	"errors"
	"sync"
)

// ConnectionRegistry owns the global logical-connection index and capacity.
type ConnectionRegistry struct {
	mu      sync.RWMutex
	max     int
	limits  ResourceLimits
	active  map[ConnectionID]*Connection
	closing map[ConnectionID]*Connection
	closed  bool
}

// NewConnectionRegistry sets connection capacity and the limits inherited by each connection.
func NewConnectionRegistry(maxConnections int, limits ResourceLimits) *ConnectionRegistry {
	return &ConnectionRegistry{
		max:     maxConnections,
		limits:  limits,
		active:  make(map[ConnectionID]*Connection),
		closing: make(map[ConnectionID]*Connection),
	}
}

// Open admits a logical connection unless shutdown or connection capacity prevents it.
func (r *ConnectionRegistry) Open() (*Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, invalidState("server is shutting down", nil)
	}

	if len(r.active)+len(r.closing) >= r.max {
		return nil, resourceExhausted("logical connection limit reached")
	}

	connection := newConnection(r.limits)
	r.active[connection.ID()] = connection

	return connection, nil
}

// Get returns an active connection; closing connections are no longer discoverable.
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

// CloseConnection commits teardown once and waits using the caller's context.
// Teardown continues if that context is cancelled.
func (r *ConnectionRegistry) CloseConnection(ctx context.Context, id ConnectionID) error {
	connection, started, err := r.beginClose(id)
	if err != nil {
		return err
	}

	if started {
		go func() {
			closeErr := connection.settleClose()
			r.remove(id, connection)
			connection.finishClose(closeErr)
		}()
	}

	return connection.waitClose(ctx)
}

// Close rejects new connections and starts teardown of all retained connections.
// The caller's context bounds waiting, not ownership of teardown.
func (r *ConnectionRegistry) Close(ctx context.Context) error {
	var result error
	for _, id := range r.beginShutdown() {
		err := r.CloseConnection(ctx, id)
		result = errors.Join(result, ignoreMissingResource(err, ErrorKindConnectionNotFound))
	}

	return result
}
