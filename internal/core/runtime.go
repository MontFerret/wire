package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/ferret/v2"
	"github.com/google/uuid"
)

// Runtime owns logical Wire connections while borrowing a host Engine.
type Runtime struct {
	mu          sync.RWMutex
	engine      *ferret.Engine
	info        RuntimeInfo
	connections map[ConnectionID]*Connection
	closing     map[ConnectionID]*Connection
	closed      bool
}

func NewRuntime(engine *ferret.Engine, info RuntimeInfo) (*Runtime, error) {
	if engine == nil {
		return nil, invalidRequest("Ferret engine is required")
	}

	return &Runtime{
		engine:      engine,
		info:        info,
		connections: make(map[ConnectionID]*Connection),
		closing:     make(map[ConnectionID]*Connection),
	}, nil
}

func (r *Runtime) Info() RuntimeInfo {
	return r.info
}

func (r *Runtime) OpenConnection() (*Connection, error) {
	id := ConnectionID(uuid.NewString())
	connection := newConnection(id, r.engine)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, invalidState("server is shutting down", nil)
	}

	r.connections[id] = connection

	return connection, nil
}

func (r *Runtime) Connection(id ConnectionID) (*Connection, error) {
	if err := validateID(id, "connection ID"); err != nil {
		return nil, err
	}

	r.mu.RLock()
	connection := r.connections[id]
	r.mu.RUnlock()
	if connection == nil {
		return nil, notFound(ErrorConnectionNotFound, string(id))
	}

	return connection, nil
}

func (r *Runtime) CloseConnection(ctx context.Context, id ConnectionID) error {
	if err := validateID(id, "connection ID"); err != nil {
		return err
	}

	r.mu.Lock()
	connection := r.connections[id]
	if connection != nil {
		delete(r.connections, id)
		r.closing[id] = connection
	} else {
		connection = r.closing[id]
	}
	r.mu.Unlock()
	if connection == nil {
		return notFound(ErrorConnectionNotFound, string(id))
	}

	return connection.Close(ctx)
}

func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	connections := make([]*Connection, 0, len(r.connections)+len(r.closing))
	for id, connection := range r.connections {
		connections = append(connections, connection)
		r.closing[id] = connection
	}
	clear(r.connections)
	for _, connection := range r.closing {
		if !containsConnection(connections, connection) {
			connections = append(connections, connection)
		}
	}
	r.mu.Unlock()

	var result error
	for _, connection := range connections {
		result = errors.Join(result, connection.Close(ctx))
	}

	return result
}

func containsConnection(values []*Connection, target *Connection) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
