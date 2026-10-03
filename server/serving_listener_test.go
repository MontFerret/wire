package server

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
)

type closeFailureListener struct {
	entered, closed chan struct{}
	failure         error
	closes          atomic.Int64
}

func (l *closeFailureListener) Accept() (net.Conn, error) {
	close(l.entered)
	<-l.closed

	return nil, net.ErrClosed
}
func (l *closeFailureListener) Close() error { l.closes.Add(1); close(l.closed); return l.failure }
func (*closeFailureListener) Addr() net.Addr { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1")} }
func TestRunListenerCloseFailureIsRetainedAndClosureRunsOnce(t *testing.T) {
	failure := errors.New("listener close failed")
	listener := &closeFailureListener{entered: make(chan struct{}), closed: make(chan struct{}), failure: failure}
	runtime := &managedRuntime{shutdownError: failure}
	s := newManagedServer(t, runtime)
	s.listen = func(context.Context, string, string) (net.Listener, error) { return listener, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx, "127.0.0.1:0") }()
	managedAwait(t, listener.entered)
	cancel()

	if err := managedAwait(t, result); !errors.Is(err, failure) {
		t.Fatal(err)
	}

	for range 2 {
		if err := s.Shutdown(managedContext(t)); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}

	if listener.closes.Load() != 1 || runtime.closes.Load() != 0 {
		t.Fatal("closure ownership violated")
	}
}
