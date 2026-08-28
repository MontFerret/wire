package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

type (
	closingConnection struct {
		connection *Connection
		close      lifecycle.Close
	}

	// Host owns logical Wire connections while borrowing a runtime.
	Host struct {
		mu          sync.RWMutex
		runtime     api.Runtime
		info        RuntimeInfo
		limits      Limits
		connections map[ConnectionID]*Connection
		closing     map[ConnectionID]*closingConnection
		closed      bool
	}
)

func NewHost(runtime api.Runtime, info RuntimeInfo, limits Limits) (*Host, error) {
	if isNil(runtime) {
		return nil, invalidRequest("runtime is required")
	}

	if err := limits.validate(); err != nil {
		return nil, err
	}

	return &Host{
		runtime:     runtime,
		info:        info,
		limits:      limits,
		connections: make(map[ConnectionID]*Connection),
		closing:     make(map[ConnectionID]*closingConnection),
	}, nil
}

func (h *Host) Info() RuntimeInfo {
	return h.info
}

func (h *Host) OpenConnection() (*Connection, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, invalidState("server is shutting down", nil)
	}

	if len(h.connections)+len(h.closing) >= h.limits.MaxConnections {
		return nil, resourceExhausted("logical connection limit reached")
	}

	id := ConnectionID(uuid.NewString())
	connection := newConnection(id, h.runtime, h.limits)

	h.connections[id] = connection

	return connection, nil
}

func (h *Host) Connection(id ConnectionID) (*Connection, error) {
	if err := validateID(id, "connection ID"); err != nil {
		return nil, err
	}

	h.mu.RLock()
	connection := h.connections[id]
	h.mu.RUnlock()
	if connection == nil {
		return nil, notFound(ErrorConnectionNotFound, string(id))
	}

	return connection, nil
}

func (h *Host) CloseConnection(ctx context.Context, id ConnectionID) error {
	if err := validateID(id, "connection ID"); err != nil {
		return err
	}

	h.mu.Lock()
	connection := h.connections[id]
	closing := h.closing[id]
	if connection != nil {
		delete(h.connections, id)
		closing = &closingConnection{connection: connection}
		h.closing[id] = closing
	}
	h.mu.Unlock()
	if closing == nil {
		return notFound(ErrorConnectionNotFound, string(id))
	}

	if closing.close.Begin() {
		go h.settleConnectionClose(id, closing)
	}

	return closing.close.Wait(ctx)
}

func (h *Host) settleConnectionClose(id ConnectionID, closing *closingConnection) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("logical connection cleanup panicked")))
		}

		h.mu.Lock()
		if h.closing[id] == closing {
			delete(h.closing, id)
		}
		h.mu.Unlock()
		closing.close.Finish(err)
	}()

	err = closing.connection.Close(context.Background())
}

func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	for id, connection := range h.connections {
		h.closing[id] = &closingConnection{connection: connection}
	}
	clear(h.connections)
	connections := make(map[ConnectionID]*closingConnection, len(h.closing))
	for id, closing := range h.closing {
		connections[id] = closing
	}
	h.mu.Unlock()

	for id, closing := range connections {
		if closing.close.Begin() {
			go h.settleConnectionClose(id, closing)
		}
	}

	var result error
	for _, closing := range connections {
		result = errors.Join(result, closing.close.Wait(ctx))
	}

	return result
}
