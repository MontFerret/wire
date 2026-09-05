package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/failure"
	"google.golang.org/grpc"
)

type (
	// Runtime is the canonical api.Runtime contract, re-exported for remote use.
	// NewRuntime returns a private Wire implementation of this interface.
	Runtime = api.Runtime

	// Session is the canonical api.Session contract for reusable normal sessions.
	// NewRuntime's plan adapters return private implementations of this interface.
	Session = api.Session

	// Output aliases api.Output, defined by github.com/MontFerret/api/result.
	// Wire preserves its content type and encoded bytes without interpretation.
	Output = api.Output

	// Client owns one logical Wire connection while borrowing its gRPC transport.
	Client struct {
		runtimeClient   wirev1.RuntimeServiceClient
		planClient      wirev1.PlanServiceClient
		sessionClient   wirev1.SessionServiceClient
		executionClient wirev1.ExecutionServiceClient
		debugClient     wirev1.DebugServiceClient

		connectionID    string
		info            RuntimeInfo
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

// New opens one logical Wire connection over a caller-owned gRPC connection.
// The construction context bounds the Connect handshake; Close owns the
// resulting long-lived logical lifecycle.
func New(ctx context.Context, connection grpc.ClientConnInterface) (*Client, error) {
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
	client := &Client{
		runtimeClient:   runtimeClient,
		planClient:      wirev1.NewPlanServiceClient(connection),
		sessionClient:   wirev1.NewSessionServiceClient(connection),
		executionClient: wirev1.NewExecutionServiceClient(connection),
		debugClient:     wirev1.NewDebugServiceClient(connection),
		connectionID:    response.GetConnectionId().GetValue(),
		info:            convertRuntimeInfo(response.GetProtocol(), response.GetRuntimeIdentity()),
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

// Run compiles and executes source once, returning the runtime's encoded output.
// It releases the Plan and Execution resources it creates before returning and
// joins operation and release errors.
func (c *Client) Run(ctx context.Context, src api.Source, parameters Parameters, options RunOptions) (Output, error) {
	plan, err := c.Compile(ctx, src, options.Compile)
	if err != nil {
		return Output{}, err
	}

	output, runErr := plan.Run(ctx, parameters, options.Execute)
	closeErr := boundedCleanup(ctx, convenienceCleanupTimeout, plan.Close)

	return output, errors.Join(runErr, closeErr)
}

// Close releases the logical Wire connection without closing the caller-owned
// gRPC transport. Concurrent callers wait for the same retained result.
func (c *Client) Close(ctx context.Context) error {
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

func (c *Client) monitorConnect() {
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

func (c *Client) checkOpen() error {
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

func (c *Client) closeResult(ctx context.Context) (bool, error) {
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

func (c *Client) retainedCloseResult() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	return c.closeErr
}

func (c *Client) connectionProto() *wirev1.ConnectionId {
	return &wirev1.ConnectionId{Value: c.connectionID}
}

func (c *Client) settleClose(ctx context.Context) {
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

func (c *Client) watchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	watch, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.lifecycleCtx, cancel)

	return watch, func() {
		stop()
		cancel()
	}
}
