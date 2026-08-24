package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/ferret/v2"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

type (
	closingConnection struct {
		connection *Connection
		close      lifecycle.Close
	}

	// Runtime owns logical Wire connections while borrowing a host Engine.
	Runtime struct {
		mu          sync.RWMutex
		engine      *ferret.Engine
		info        RuntimeInfo
		limits      Limits
		connections map[ConnectionID]*Connection
		closing     map[ConnectionID]*closingConnection
		closed      bool
	}
)

func NewRuntime(engine *ferret.Engine, info RuntimeInfo, limits Limits) (*Runtime, error) {
	if engine == nil {
		return nil, invalidRequest("Ferret engine is required")
	}

	if err := limits.validate(); err != nil {
		return nil, err
	}

	return &Runtime{
		engine:      engine,
		info:        info,
		limits:      limits,
		connections: make(map[ConnectionID]*Connection),
		closing:     make(map[ConnectionID]*closingConnection),
	}, nil
}

func (r *Runtime) Info() RuntimeInfo {
	return r.info
}

func (r *Runtime) OpenConnection() (*Connection, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, invalidState("server is shutting down", nil)
	}

	if len(r.connections)+len(r.closing) >= r.limits.MaxConnections {
		return nil, resourceExhausted("logical connection limit reached")
	}

	id := ConnectionID(uuid.NewString())
	connection := newConnection(id, r.engine, r.limits)

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
	closing := r.closing[id]
	if connection != nil {
		delete(r.connections, id)
		closing = &closingConnection{connection: connection}
		r.closing[id] = closing
	}
	r.mu.Unlock()
	if closing == nil {
		return notFound(ErrorConnectionNotFound, string(id))
	}

	if closing.close.Begin() {
		go r.settleConnectionClose(id, closing)
	}

	return closing.close.Wait(ctx)
}

func (r *Runtime) settleConnectionClose(id ConnectionID, closing *closingConnection) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("logical connection cleanup panicked")))
		}

		r.mu.Lock()
		if r.closing[id] == closing {
			delete(r.closing, id)
		}
		r.mu.Unlock()
		closing.close.Finish(err)
	}()

	err = closing.connection.Close(context.Background())
}

func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	for id, connection := range r.connections {
		r.closing[id] = &closingConnection{connection: connection}
	}
	clear(r.connections)
	connections := make(map[ConnectionID]*closingConnection, len(r.closing))
	for id, closing := range r.closing {
		connections[id] = closing
	}
	r.mu.Unlock()

	for id, closing := range connections {
		if closing.close.Begin() {
			go r.settleConnectionClose(id, closing)
		}
	}

	var result error
	for _, closing := range connections {
		result = errors.Join(result, closing.close.Wait(ctx))
	}

	return result
}
