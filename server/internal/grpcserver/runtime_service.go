package grpcserver

import (
	"context"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/server/internal/core"
)

// RuntimeService adapts the runtime RPC contract to its core owners.
type RuntimeService struct {
	wirev1.UnimplementedRuntimeServiceServer
	info        Handshake
	runtime     api.Runtime
	connections *core.ConnectionRegistry
}

var _ wirev1.RuntimeServiceServer = (*RuntimeService)(nil)

func (s *RuntimeService) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	connection, err := s.connections.Open()
	if err != nil {
		return rpcError(err)
	}

	defer func() {
		_ = s.connections.CloseConnection(context.Background(), connection.ID())
	}()

	response := &wirev1.ConnectResponse{
		ConnectionId:    &wirev1.ConnectionId{Value: string(connection.ID())},
		Protocol:        protocolInfo(s.info),
		RuntimeIdentity: runtimeIdentity(s.info),
	}

	if err := stream.Send(response); err != nil {
		return err
	}

	select {
	case <-stream.Context().Done():
		return nil
	case <-connection.Context().Done():
		return nil
	}
}

func (s *RuntimeService) CloseConnection(ctx context.Context, request *wirev1.CloseConnectionRequest) (*wirev1.CloseConnectionResponse, error) {
	err := s.connections.CloseConnection(ctx, core.ConnectionID(request.GetConnectionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.CloseConnectionResponse{}, nil
}

func (s *RuntimeService) Run(
	ctx context.Context,
	request *wirev1.RunRequest,
) (*wirev1.RunResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	options, err := decodeSessionOptions(request.GetParameters(), request.GetOutputContentType())
	if err != nil {
		return nil, rpcError(err)
	}

	created, err := core.Run(operation, s.runtime, resources, decodeSource(request.GetSource()), options...)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := execution(created.ID(), wireexecution.Snapshot{State: wireexecution.StateRunning})
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.RunResponse{Execution: converted}, nil
}
