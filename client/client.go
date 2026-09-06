package client

import (
	"context"
	"errors"
	"io"
	"sync"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/failure"
	"google.golang.org/grpc"
)

type (
	// connectionHandle owns one logical connection and borrows its gRPC transport.
	connectionHandle struct {
		runtimeClient   wirev1.RuntimeServiceClient
		planClient      wirev1.PlanServiceClient
		sessionClient   wirev1.SessionServiceClient
		executionClient wirev1.ExecutionServiceClient
		debugClient     wirev1.DebugServiceClient

		connectionID    string
		stream          wirev1.RuntimeService_ConnectClient
		streamCancel    context.CancelFunc
		streamDone      chan struct{}
		streamMu        sync.Mutex
		streamErr       error
		lifecycleCtx    context.Context
		lifecycleCancel context.CancelFunc

		closeOnce sync.Once
		closeDone chan struct{}
		closeMu   sync.Mutex
		closeErr  error
		closing   bool
	}
)

// newConnection opens one logical Wire connection over a caller-owned gRPC connection.
// The construction context bounds the Connect handshake; Close owns the
// resulting long-lived logical lifecycle.
func newConnection(ctx context.Context, connection grpc.ClientConnInterface) (*connectionHandle, error) {
	if connection == nil {
		return nil, errors.New("gRPC connection is required")
	}

	runtimeClient := wirev1.NewRuntimeServiceClient(connection)
	streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := runtimeClient.Connect(streamCtx, &wirev1.ConnectRequest{})
	if err != nil {
		streamCancel()

		return nil, decodeError(err)
	}

	type firstResult struct {
		response *wirev1.ConnectResponse
		err      error
	}

	first := make(chan firstResult, 1)

	go func() {
		response, receiveErr := stream.Recv()
		first <- firstResult{response: response, err: receiveErr}
	}()

	var response *wirev1.ConnectResponse
	select {
	case <-ctx.Done():
		streamCancel()

		return nil, ctx.Err()
	case result := <-first:
		if result.err != nil {
			streamCancel()

			return nil, decodeError(result.err)
		}

		response = result.response
	}

	if response.GetConnectionId().GetValue() == "" || response.GetProtocol() == nil ||
		response.GetProtocol().GetName() == "" || response.GetProtocol().GetVersion() == "" {
		streamCancel()

		return nil, errors.New("Wire server returned an invalid Connect handshake")
	}

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	client := &connectionHandle{
		runtimeClient:   runtimeClient,
		planClient:      wirev1.NewPlanServiceClient(connection),
		sessionClient:   wirev1.NewSessionServiceClient(connection),
		executionClient: wirev1.NewExecutionServiceClient(connection),
		debugClient:     wirev1.NewDebugServiceClient(connection),
		connectionID:    response.GetConnectionId().GetValue(),
		stream:          stream,
		streamCancel:    streamCancel,
		streamDone:      make(chan struct{}),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
		closeDone:       make(chan struct{}),
	}

	go client.monitorConnect()

	return client, nil
}

// Close releases the logical Wire connection without closing the caller-owned
// gRPC transport. Concurrent callers wait for the same retained result.
func (c *connectionHandle) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		c.closeMu.Lock()
		c.closing = true
		c.closeMu.Unlock()

		go func() {
			retained, cancel := retainedContext(ctx)
			defer cancel()
			c.settleClose(retained)
		}()
	})

	select {
	case <-c.closeDone:
		return c.retainedCloseResult()
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeDone:
		return c.retainedCloseResult()
	}
}

func (c *connectionHandle) monitorConnect() {
	_, err := c.stream.Recv()

	c.streamMu.Lock()
	if err == nil {
		c.streamErr = errors.New("Wire server returned an unexpected Connect response")
	} else if !errors.Is(err, io.EOF) {
		c.streamErr = decodeError(err)
	}

	c.streamMu.Unlock()

	c.lifecycleCancel()
	close(c.streamDone)
}

func (c *connectionHandle) checkOpen() error {
	if c == nil {
		return ErrClosed
	}

	c.closeMu.Lock()
	closing := c.closing
	c.closeMu.Unlock()
	if closing {
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

func (c *connectionHandle) closeResult(ctx context.Context) (bool, error) {
	if c == nil {
		return true, ErrClosed
	}

	c.closeMu.Lock()
	closing := c.closing
	c.closeMu.Unlock()
	if !closing {
		return false, nil
	}

	select {
	case <-c.closeDone:
		return true, c.retainedCloseResult()
	default:
	}

	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-c.closeDone:
		return true, c.retainedCloseResult()
	}
}

func (c *connectionHandle) retainedCloseResult() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	return c.closeErr
}

func (c *connectionHandle) connectionProto() *wirev1.ConnectionId {
	return &wirev1.ConnectionId{Value: c.connectionID}
}

func (c *connectionHandle) settleClose(ctx context.Context) {
	var result error
	defer func() {
		if recover() != nil {
			result = errors.Join(result, errors.New("Wire client close panicked"))
		}

		c.streamCancel()
		c.lifecycleCancel()
		c.closeMu.Lock()
		c.closeErr = result
		c.closeMu.Unlock()
		close(c.closeDone)
	}()

	_, err := c.runtimeClient.CloseConnection(ctx, &wirev1.CloseConnectionRequest{ConnectionId: c.connectionProto()})
	result = decodeError(err)

	var wireErr *Error
	if errors.As(result, &wireErr) && wireErr.Category == failure.CategoryConnectionNotFound {
		result = nil
	}

	c.streamCancel()
	c.lifecycleCancel()

	select {
	case <-c.streamDone:
	case <-ctx.Done():
	}
}

func (c *connectionHandle) watchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	watch, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.lifecycleCtx, cancel)

	return watch, func() {
		stop()
		cancel()
	}
}
