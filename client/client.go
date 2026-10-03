package client

import (
	"context"
	"errors"
	"io"
	"sync"

	gooptions "github.com/ziflex/go-options"
	"google.golang.org/grpc"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/failure"
)

type (
	// connectionHandle owns one logical connection and its optional transport teardown.
	connectionHandle struct {
		runtimeClient   wirev1.RuntimeServiceClient
		planClient      wirev1.PlanServiceClient
		sessionClient   wirev1.SessionServiceClient
		executionClient wirev1.ExecutionServiceClient
		debugClient     wirev1.DebugServiceClient

		connectionID    string
		runtimeVersion  api.Version
		stream          wirev1.RuntimeService_ConnectClient
		streamCancel    context.CancelFunc
		streamDone      chan struct{}
		streamMu        sync.Mutex
		streamErr       error
		lifecycleCtx    context.Context
		lifecycleCancel context.CancelFunc

		closeOnce      sync.Once
		closeDone      chan struct{}
		closeMu        sync.Mutex
		closeErr       error
		closing        bool
		terminal       bool // Connect ended before close committed; preserve its error.
		transportClose func() error
	}
)

// newConnection establishes the startup-only cancellation link before stream
// creation. One teardown owner handles both rollback and published lifetimes.
func newConnection(ctx context.Context, connection grpc.ClientConnInterface, transportClose func() error) (result *connectionHandle, resultErr error) {
	base := ctx
	if base == nil {
		base = context.Background()
	}

	streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(base))
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.WithoutCancel(base))
	client := &connectionHandle{
		runtimeClient:   wirev1.NewRuntimeServiceClient(connection),
		planClient:      wirev1.NewPlanServiceClient(connection),
		sessionClient:   wirev1.NewSessionServiceClient(connection),
		executionClient: wirev1.NewExecutionServiceClient(connection),
		debugClient:     wirev1.NewDebugServiceClient(connection),
		streamCancel:    streamCancel,
		streamDone:      make(chan struct{}),
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
		closeDone:       make(chan struct{}),
		transportClose:  transportClose,
	}

	defer func() {
		if resultErr != nil {
			// No monitor has been published; the constructor's reader is done.
			close(client.streamDone)
			resultErr = errors.Join(resultErr, boundedCleanup(base, convenienceCleanupTimeout, client.Close))
		}
	}()

	if err := runtimeContextError(ctx); err != nil {
		return nil, err
	}

	if err := gooptions.NotNil[grpc.ClientConnInterface]()(connection); err != nil {
		return nil, errors.New("gRPC connection is required")
	}

	stopStartup := context.AfterFunc(ctx, streamCancel)
	defer stopStartup()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	stream, err := client.runtimeClient.Connect(streamCtx, &wirev1.ConnectRequest{})
	if err != nil {
		return nil, errors.Join(ctx.Err(), decodeError(err))
	}

	client.stream = stream

	response, err := stream.Recv()
	if err != nil {
		return nil, errors.Join(ctx.Err(), decodeError(err))
	}

	client.connectionID = response.GetConnectionId().GetValue()
	if client.connectionID == "" || response.GetProtocol() == nil ||
		response.GetProtocol().GetName() == "" || response.GetProtocol().GetVersion() == "" || response.RuntimeVersion == nil {
		return nil, errors.Join(ctx.Err(), errors.New("Wire server returned an invalid Connect handshake"))
	}

	client.runtimeVersion = api.Version(response.GetRuntimeVersion())
	stopStartup()

	// This check commits successful publication. Later startup cancellation has
	// no link to the connection, its operations, or its descendants.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	go client.monitorConnect()

	return client, nil
}

// Close releases the logical connection and then any owned transport. Borrowed
// transports remain open. Concurrent callers wait for the same retained result.
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

	c.closeMu.Lock()
	terminal := !c.closing
	c.terminal = terminal
	c.closeMu.Unlock()

	c.lifecycleCancel()
	close(c.streamDone)

	// Publish reader completion before teardown can wait for it. Definitive
	// stream loss invalidates the logical lifetime even when resources remain.
	if terminal {
		_ = boundedCleanup(c.lifecycleCtx, convenienceCleanupTimeout, c.Close)
	}
}

func (c *connectionHandle) checkOpen() error {
	if c == nil {
		return ErrClosed
	}

	c.closeMu.Lock()
	closing, terminal := c.closing, c.terminal
	c.closeMu.Unlock()

	if closing || terminal {
		if terminal {
			c.streamMu.Lock()
			err := c.streamErr
			c.streamMu.Unlock()

			if err != nil {
				return err
			}
		}

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
		result = errors.Join(result, c.closeTransport())

		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(result, ctxErr) {
			result = errors.Join(result, ctxErr)
		}

		c.closeMu.Lock()
		c.closeErr = result
		c.closeMu.Unlock()
		close(c.closeDone)
	}()

	if c.connectionID != "" {
		_, err := c.runtimeClient.CloseConnection(ctx, &wirev1.CloseConnectionRequest{ConnectionId: c.connectionProto()})
		result = decodeError(err)

		var wireErr *Error
		if errors.As(result, &wireErr) && wireErr.Category == failure.CategoryConnectionNotFound {
			result = nil
		}
	}

	c.streamCancel()
	c.lifecycleCancel()

	select {
	case <-c.streamDone:
	case <-ctx.Done():
	}
}

func (c *connectionHandle) closeTransport() (result error) {
	if c.transportClose == nil {
		return nil
	}

	defer func() {
		if recover() != nil {
			result = errors.New("Wire client transport close panicked")
		}
	}()

	return c.transportClose()
}

func (c *connectionHandle) watchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	watch, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.lifecycleCtx, cancel)

	return watch, func() {
		stop()
		cancel()
	}
}
