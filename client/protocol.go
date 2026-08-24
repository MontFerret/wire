package client

import (
	"context"
	"errors"
	"io"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
)

type protocolClient struct {
	runtimeClient   wirev1.RuntimeServiceClient
	planClient      wirev1.PlanServiceClient
	executionClient wirev1.ExecutionServiceClient
	debugClient     wirev1.DebugServiceClient
	connectionID    string
	connectStream   wirev1.RuntimeService_ConnectClient
	connectCancel   context.CancelFunc
}

func openProtocol(ctx context.Context, connection grpc.ClientConnInterface) (*protocolClient, RuntimeInfo, error) {
	runtimeClient := wirev1.NewRuntimeServiceClient(connection)
	streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := runtimeClient.Connect(streamCtx, &wirev1.ConnectRequest{})
	if err != nil {
		streamCancel()

		return nil, RuntimeInfo{}, decodeError(err)
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

		return nil, RuntimeInfo{}, ctx.Err()
	case result := <-first:
		if result.err != nil {
			streamCancel()

			return nil, RuntimeInfo{}, decodeError(result.err)
		}

		response = result.response
	}

	opened := response.GetOpened()
	if opened == nil || opened.GetConnectionId().GetValue() == "" || opened.GetRuntimeInfo() == nil {
		streamCancel()

		return nil, RuntimeInfo{}, errors.New("Wire server returned an invalid Connect handshake")
	}

	protocol := &protocolClient{
		runtimeClient:   runtimeClient,
		planClient:      wirev1.NewPlanServiceClient(connection),
		executionClient: wirev1.NewExecutionServiceClient(connection),
		debugClient:     wirev1.NewDebugServiceClient(connection),
		connectionID:    opened.GetConnectionId().GetValue(),
		connectStream:   stream,
		connectCancel:   streamCancel,
	}

	return protocol, convertRuntimeInfo(opened.GetRuntimeInfo()), nil
}

func (p *protocolClient) monitorConnection() error {
	_, err := p.connectStream.Recv()
	if err == nil {
		return errors.New("Wire server returned an unexpected Connect response")
	}

	if errors.Is(err, io.EOF) {
		return nil
	}

	return decodeError(err)
}

func (p *protocolClient) closeConnection(ctx context.Context) error {
	_, err := p.runtimeClient.CloseConnection(ctx, &wirev1.CloseConnectionRequest{
		ConnectionId: p.connectionProto(),
	})

	return decodeError(err)
}

func (p *protocolClient) cancelConnection() {
	p.connectCancel()
}

func (p *protocolClient) connectionProto() *wirev1.ConnectionId {
	return &wirev1.ConnectionId{Value: p.connectionID}
}

func convertRuntimeInfo(value *wirev1.RuntimeInfo) RuntimeInfo {
	result := RuntimeInfo{
		APIIdentity:   value.GetApiIdentity(),
		WireVersion:   value.GetWireVersion(),
		FerretVersion: value.GetFerretVersion(),
	}

	if identity := value.GetRuntimeIdentity(); identity != nil {
		result.RuntimeIdentity = &RuntimeIdentity{
			Name:       identity.GetName(),
			Version:    identity.GetVersion(),
			InstanceID: identity.GetInstanceId(),
		}
	}

	for _, capability := range value.GetCapabilities() {
		switch capability {
		case wirev1.Capability_CAPABILITY_EXECUTION:
			result.Capabilities.Execution = true
		case wirev1.Capability_CAPABILITY_DEBUGGING:
			result.Capabilities.Debugging = true
		case wirev1.Capability_CAPABILITY_CANCELLATION:
			result.Capabilities.Cancellation = true
		}
	}

	return result
}
