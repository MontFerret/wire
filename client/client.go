package client

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/wire/internal/lifecycle"
	"google.golang.org/grpc"
)

type (
	// Client owns one logical Wire connection while borrowing its gRPC transport.
	Client struct {
		session    *session
		plans      *planTransport
		executions *executionTransport
		debug      *debugTransport
		info       RuntimeInfo
		handle     *lifecycle.Handle

		streamDone chan struct{}
		streamMu   sync.Mutex
		streamErr  error

		lifecycleCtx    context.Context
		lifecycleCancel context.CancelFunc
	}

	// RunOptions composes plan compilation and execution options for Client.Run.
	RunOptions struct {
		// Compile controls construction of the temporary plan.
		Compile CompileOptions
		// Execute controls the temporary execution and its encoded output.
		Execute ExecuteOptions
	}
)

// New opens one logical Wire connection over a caller-owned gRPC connection.
// The construction context bounds the Connect handshake; Close owns the
// resulting long-lived logical lifecycle.
func New(ctx context.Context, connection grpc.ClientConnInterface) (*Client, error) {
	if connection == nil {
		return nil, errors.New("gRPC connection is required")
	}

	session, info, err := openSession(ctx, connection)
	if err != nil {
		return nil, err
	}

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	client := &Client{
		session:         session,
		plans:           newPlanTransport(connection, session),
		executions:      newExecutionTransport(connection, session),
		debug:           newDebugTransport(connection, session),
		info:            info,
		handle:          &lifecycle.Handle{},
		streamDone:      make(chan struct{}),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}

	go client.monitorConnect()

	return client, nil
}

// RuntimeInfo returns a copy of the server and host information published by
// the Connect handshake.
func (c *Client) RuntimeInfo() RuntimeInfo {
	result := c.info

	if c.info.RuntimeIdentity != nil {
		identity := *c.info.RuntimeIdentity
		result.RuntimeIdentity = &identity
	}

	return result
}

// Run compiles and executes source once, returning Ferret's encoded output.
// It releases the Plan and Execution resources it creates before returning and
// joins operation and release errors.
func (c *Client) Run(ctx context.Context, source Source, parameters Parameters, options RunOptions) (Output, error) {
	plan, err := c.Compile(ctx, source, options.Compile)
	if err != nil {
		return Output{}, err
	}

	output, runErr := plan.Run(ctx, parameters, options.Execute)
	closeErr := plan.Close(context.WithoutCancel(ctx))

	return output, errors.Join(runErr, closeErr)
}

// Close releases the logical Wire connection without closing the caller-owned
// gRPC transport. Concurrent callers wait for the same retained result.
func (c *Client) Close(ctx context.Context) error {
	if c == nil || c.session == nil || c.handle == nil {
		return ErrClosed
	}

	return c.handle.Close(ctx, c.release)
}

func (c *Client) monitorConnect() {
	err := c.session.monitor()

	c.streamMu.Lock()
	c.streamErr = err
	c.streamMu.Unlock()

	c.lifecycleCancel()
	close(c.streamDone)
}

func (c *Client) checkOpen() error {
	if c == nil || c.session == nil || c.handle == nil || !c.handle.Open() {
		return ErrClosed
	}

	select {
	case <-c.streamDone:
		c.streamMu.Lock()
		err := c.streamErr
		c.streamMu.Unlock()
		if err != nil {
			return err
		}

		return ErrClosed
	default:
		return nil
	}
}

func (c *Client) closeResult(ctx context.Context) (bool, error) {
	if c == nil || c.session == nil || c.handle == nil {
		return true, ErrClosed
	}

	return c.handle.CloseResult(ctx)
}

func (c *Client) release(ctx context.Context) error {
	defer func() {
		c.session.cancel()
		c.lifecycleCancel()
	}()

	result := c.session.close(ctx)

	var wireErr *Error
	if errors.As(result, &wireErr) && wireErr.Category == ErrorConnectionNotFound {
		result = nil
	}

	c.session.cancel()
	c.lifecycleCancel()

	select {
	case <-c.streamDone:
	case <-ctx.Done():
	}

	return result
}

func (c *Client) watchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	watch, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.lifecycleCtx, cancel)

	return watch, func() {
		stop()
		cancel()
	}
}
