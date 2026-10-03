package harness

import (
	"net"
	"sync"
)

type (
	observedListener struct {
		net.Listener
		ends chan<- struct{}
	}

	observedConnection struct {
		net.Conn
		ends chan<- struct{}
		once sync.Once
	}
)

func (l *observedListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return &observedConnection{Conn: connection, ends: l.ends}, nil
}

func (c *observedConnection) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		select {
		case c.ends <- struct{}{}:
		default:
		}
	})

	return err
}

// TransportClosed reports server-side socket closure for an owned-channel
// fixture. Tests observe it before server shutdown can conceal leaked channels.
func (h *Harness) TransportClosed() <-chan struct{} {
	return h.transportEnds
}

// Endpoint returns the open loopback listener address for an owned fixture.
func (h *Harness) Endpoint() string {
	return h.endpoint
}
