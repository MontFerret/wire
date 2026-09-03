package core

import "context"

// Context is one Wire operation scoped to a logical connection.
type Context struct {
	context.Context
	connection *Connection
}

// NewContext combines caller cancellation with the logical connection
// lifetime. The returned cancel function must be called when the operation
// finishes so the connection cancellation callback is detached.
func NewContext(parent context.Context, connection *Connection) (*Context, context.CancelFunc) {
	operation, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(connection.Context(), func() {
		cancel(context.Cause(connection.Context()))
	})
	if err := connection.Context().Err(); err != nil {
		cancel(context.Cause(connection.Context()))
	}

	return &Context{Context: operation, connection: connection}, func() {
		stop()
		cancel(context.Canceled)
	}
}

func (c *Context) Connection() *Connection {
	return c.connection
}

func (c *Context) connectionID() ConnectionID {
	return c.connection.ID()
}
