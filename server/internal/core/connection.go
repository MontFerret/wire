package core

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/MontFerret/wire/server/internal/lifecycle"
)

// Connection is the logical identity and lifetime established by the Connect
// stream. Its store owns allocation and reclamation within this lifetime.
type Connection struct {
	id        ConnectionID
	ctx       context.Context
	cancel    context.CancelCauseFunc
	resources *ResourceStore
	close     lifecycle.Close
}

func newConnection(limits ResourceLimits) *Connection {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &Connection{
		id:        ConnectionID(uuid.NewString()),
		ctx:       ctx,
		cancel:    cancel,
		resources: newResourceStore(ctx, limits),
	}
}

// ID identifies this logical connection independently of its physical transport.
func (c *Connection) ID() ConnectionID {
	return c.id
}

// Context is cancelled when logical connection teardown begins.
func (c *Connection) Context() context.Context {
	return c.ctx
}

// Resources returns the store owned by this logical connection.
func (c *Connection) Resources() *ResourceStore {
	return c.resources
}

func (c *Connection) settleClose() (err error) {
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("logical connection cleanup panicked")))
		}
	}()

	err = c.resources.Close(context.Background())

	return err
}

// beginClose linearizes connection cancellation against operation admission.
// The caller that receives true owns cross-resource teardown.
func (c *Connection) beginClose() bool {
	c.resources.mu.Lock()
	defer c.resources.mu.Unlock()

	if !c.close.Begin() {
		return false
	}

	c.cancel(context.Canceled)

	return true
}

func (c *Connection) finishClose(err error) {
	c.close.Finish(err)
}

func (c *Connection) waitClose(ctx context.Context) error {
	return c.close.Wait(ctx)
}
