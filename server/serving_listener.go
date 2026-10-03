package server

import (
	"errors"
	"net"
	"sync"
)

// servingListener retains fatal accept errors even if concurrent gRPC shutdown
// masks them. Closure is shared by gRPC and the serving invocation.
type servingListener struct {
	net.Listener
	closeOnce sync.Once
	mu        sync.Mutex
	closed    bool
	acceptErr error
	closeErr  error
}

func (l *servingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		return connection, nil
	}

	if temporary, ok := err.(interface{ Temporary() bool }); ok && temporary.Temporary() {
		return nil, err
	}

	l.mu.Lock()
	if !l.closed || !errors.Is(err, net.ErrClosed) {
		l.acceptErr = err
	}

	l.mu.Unlock()

	return nil, err
}

func (l *servingListener) Close() error {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		l.closed = true
		l.mu.Unlock()

		err := l.Listener.Close()
		if errors.Is(err, net.ErrClosed) {
			err = nil
		}

		l.mu.Lock()
		l.closeErr = err
		l.mu.Unlock()
	})
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.closeErr
}

func (l *servingListener) result(serveErr error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed && errors.Is(serveErr, net.ErrClosed) {
		serveErr = nil
	}

	if l.acceptErr != nil && errors.Is(serveErr, l.acceptErr) {
		serveErr = nil
	}

	return errors.Join(serveErr, l.acceptErr)
}
