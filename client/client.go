package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
)

type Option interface {
	apply(*clientOptions) error
}

type optionFunc func(*clientOptions) error

func (option optionFunc) apply(options *clientOptions) error {
	return option(options)
}

type clientOptions struct {
	identity ClientIdentity
}

func WithClientIdentity(identity ClientIdentity) Option {
	return optionFunc(func(options *clientOptions) error {
		if identity.Name == "" {
			return errors.New("client identity name is required")
		}
		options.identity = identity
		return nil
	})
}

// Client owns one logical Wire connection while borrowing its gRPC transport.
type Client struct {
	runtimeClient   wirev1.RuntimeServiceClient
	planClient      wirev1.PlanServiceClient
	executionClient wirev1.ExecutionServiceClient
	debugClient     wirev1.DebugServiceClient

	connectionID ConnectionID
	info         RuntimeInfo
	stream       wirev1.RuntimeService_ConnectClient
	streamCancel context.CancelFunc
	streamDone   chan struct{}
	streamMu     sync.Mutex
	streamErr    error

	closeOnce sync.Once
	closeDone chan struct{}
	closeMu   sync.Mutex
	closeErr  error
}

func New(ctx context.Context, connection grpc.ClientConnInterface, options ...Option) (*Client, error) {
	if connection == nil {
		return nil, errors.New("gRPC connection is required")
	}
	configured := clientOptions{}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("client option must not be nil")
		}
		if err := option.apply(&configured); err != nil {
			return nil, err
		}
	}

	runtimeClient := wirev1.NewRuntimeServiceClient(connection)
	streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := runtimeClient.Connect(streamCtx, &wirev1.ConnectRequest{ClientIdentity: &wirev1.ClientIdentity{
		Name:    configured.identity.Name,
		Version: configured.identity.Version,
	}})
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
	opened := response.GetOpened()
	if opened == nil || opened.GetConnectionId().GetValue() == "" || opened.GetRuntimeInfo() == nil {
		streamCancel()
		return nil, errors.New("Wire server returned an invalid Connect handshake")
	}

	client := &Client{
		runtimeClient:   runtimeClient,
		planClient:      wirev1.NewPlanServiceClient(connection),
		executionClient: wirev1.NewExecutionServiceClient(connection),
		debugClient:     wirev1.NewDebugServiceClient(connection),
		connectionID:    ConnectionID(opened.GetConnectionId().GetValue()),
		info:            convertRuntimeInfo(opened.GetRuntimeInfo()),
		stream:          stream,
		streamCancel:    streamCancel,
		streamDone:      make(chan struct{}),
		closeDone:       make(chan struct{}),
	}
	go client.monitorConnect()

	return client, nil
}

func (c *Client) monitorConnect() {
	for {
		_, err := c.stream.Recv()
		if err == nil {
			continue
		}
		c.streamMu.Lock()
		if !errors.Is(err, io.EOF) {
			c.streamErr = decodeError(err)
		}
		c.streamMu.Unlock()
		close(c.streamDone)
		return
	}
}

func (c *Client) ConnectionID() ConnectionID {
	return c.connectionID
}

func (c *Client) RuntimeInfo() RuntimeInfo {
	result := c.info
	if c.info.RuntimeIdentity != nil {
		identity := *c.info.RuntimeIdentity
		result.RuntimeIdentity = &identity
	}
	return result
}

func (c *Client) checkOpen() error {
	select {
	case <-c.streamDone:
		c.streamMu.Lock()
		err := c.streamErr
		c.streamMu.Unlock()
		if err != nil {
			return err
		}
		return errors.New("Wire logical connection is closed")
	default:
		return nil
	}
}

func (c *Client) connectionProto() *wirev1.ConnectionId {
	return &wirev1.ConnectionId{Value: string(c.connectionID)}
}

// Close releases the logical Wire connection without closing the caller-owned
// gRPC transport. Concurrent callers wait for the same retained result.
func (c *Client) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		go func() {
			retained, cancel := retainedContext(ctx)
			defer cancel()
			c.settleClose(retained)
		}()
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeDone:
		c.closeMu.Lock()
		err := c.closeErr
		c.closeMu.Unlock()
		return err
	}
}

func retainedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.Background(), func() {}
	}
	return context.WithDeadline(context.Background(), deadline)
}

func (c *Client) settleClose(ctx context.Context) {
	_, err := c.runtimeClient.CloseConnection(ctx, &wirev1.CloseConnectionRequest{ConnectionId: c.connectionProto()})
	c.streamCancel()
	select {
	case <-c.streamDone:
	case <-ctx.Done():
	}
	c.closeMu.Lock()
	c.closeErr = decodeError(err)
	c.closeMu.Unlock()
	close(c.closeDone)
}

func (c *Client) Compile(ctx context.Context, source Source, options CompileOptions) (Plan, error) {
	if err := c.checkOpen(); err != nil {
		return Plan{}, err
	}
	response, err := c.planClient.Compile(ctx, &wirev1.CompileRequest{
		ConnectionId: c.connectionProto(),
		Source:       &wirev1.Source{Content: source.Content, Identity: source.Identity},
		Options:      &wirev1.CompileOptions{Debuggable: options.Debuggable},
	})
	if err != nil {
		return Plan{}, decodeError(err)
	}
	if response.GetPlan() == nil {
		return Plan{}, errors.New("Wire server returned no compiled plan")
	}
	return convertPlan(response.GetPlan()), nil
}

func (c *Client) ReleasePlan(ctx context.Context, id PlanID) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	_, err := c.planClient.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{
		ConnectionId: c.connectionProto(),
		PlanId:       &wirev1.PlanId{Value: string(id)},
	})
	return decodeError(err)
}

func (c *Client) Execute(ctx context.Context, id PlanID, parameters Parameters, options ExecuteOptions) (Execution, error) {
	if err := c.checkOpen(); err != nil {
		return Execution{}, err
	}
	converted, err := encodeParameters(parameters)
	if err != nil {
		return Execution{}, err
	}
	response, err := c.executionClient.Execute(ctx, &wirev1.ExecuteRequest{
		ConnectionId:      c.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: string(id)},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return Execution{}, decodeError(err)
	}
	if response.GetExecution() == nil {
		return Execution{}, errors.New("Wire server returned no execution")
	}
	return convertExecution(response.GetExecution()), nil
}

func (c *Client) CancelExecution(ctx context.Context, id ExecutionID) (Execution, error) {
	if err := c.checkOpen(); err != nil {
		return Execution{}, err
	}
	response, err := c.executionClient.CancelExecution(ctx, &wirev1.CancelExecutionRequest{
		ConnectionId: c.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: string(id)},
	})
	if err != nil {
		return Execution{}, decodeError(err)
	}
	return convertExecution(response.GetExecution()), nil
}

func (c *Client) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	_, err := c.executionClient.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{
		ConnectionId: c.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: string(id)},
	})
	return decodeError(err)
}

type ExecutionEvents struct {
	stream wirev1.ExecutionService_WatchExecutionClient
}

func (c *Client) WatchExecution(ctx context.Context, id ExecutionID) (*ExecutionEvents, error) {
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	stream, err := c.executionClient.WatchExecution(ctx, &wirev1.WatchExecutionRequest{
		ConnectionId: c.connectionProto(),
		ExecutionId:  &wirev1.ExecutionId{Value: string(id)},
	})
	if err != nil {
		return nil, decodeError(err)
	}
	return &ExecutionEvents{stream: stream}, nil
}

func (events *ExecutionEvents) Recv() (ExecutionEvent, error) {
	if events == nil || events.stream == nil {
		return ExecutionEvent{}, errors.New("execution event receiver is nil")
	}
	value, err := events.stream.Recv()
	if err != nil {
		return ExecutionEvent{}, decodeError(err)
	}
	if value.GetPayload() == nil {
		return ExecutionEvent{}, fmt.Errorf("Wire server returned an empty execution event")
	}
	return convertExecutionEvent(value), nil
}
