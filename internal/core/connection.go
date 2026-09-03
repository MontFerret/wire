package core

import (
	"context"
	"sync"

	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

type (
	connectionState uint8

	// Connection is the logical identity and lifetime established by
	// RuntimeService.Connect. Resource ownership is represented in the global
	// registries rather than by child collections on Connection.
	Connection struct {
		mu         sync.RWMutex
		id         ConnectionID
		ctx        context.Context
		cancel     context.CancelCauseFunc
		state      connectionState
		operations sync.WaitGroup
		close      lifecycle.Close
	}
)

const (
	connectionOpen connectionState = iota + 1
	connectionClosing
	connectionClosed
)

func NewConnection() *Connection {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &Connection{
		id:     ConnectionID(uuid.NewString()),
		ctx:    ctx,
		cancel: cancel,
		state:  connectionOpen,
	}
}

func (c *Connection) ID() ConnectionID {
	return c.id
}

func (c *Connection) Context() context.Context {
	return c.ctx
}

func (c *Connection) beginOperation() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != connectionOpen {
		return invalidState("connection is closed", context.Canceled)
	}

	c.operations.Add(1)

	return nil
}

func (c *Connection) finishOperation() {
	c.operations.Done()
}

// beginClose linearizes connection cancellation against operation admission.
// The caller that receives true owns cross-resource teardown.
func (c *Connection) beginClose() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.close.Begin() {
		return false
	}

	c.state = connectionClosing
	c.cancel(context.Canceled)

	return true
}

func (c *Connection) waitOperations() {
	c.operations.Wait()
}

func (c *Connection) finishClose(err error) {
	c.mu.Lock()
	c.state = connectionClosed
	c.mu.Unlock()

	c.close.Finish(err)
}

func (c *Connection) waitClose(ctx context.Context) error {
	return c.close.Wait(ctx)
}
