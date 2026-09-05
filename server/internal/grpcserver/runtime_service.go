package grpcserver

import (
	"context"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func (s *Server) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	connection := core.NewConnection()
	if err := s.connections.Register(connection); err != nil {
		return rpcError(err)
	}

	defer func() {
		_ = s.lifecycle.CloseConnection(context.Background(), connection.ID())
	}()

	response := &wirev1.ConnectResponse{
		ConnectionId:    &wirev1.ConnectionId{Value: string(connection.ID())},
		Protocol:        protocolInfo(s.info),
		RuntimeIdentity: runtimeIdentity(s.info.RuntimeIdentity),
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

func (s *Server) CloseConnection(ctx context.Context, request *wirev1.CloseConnectionRequest) (*wirev1.CloseConnectionResponse, error) {
	err := s.lifecycle.CloseConnection(ctx, core.ConnectionID(request.GetConnectionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.CloseConnectionResponse{}, nil
}

func (s *Server) Run(
	ctx context.Context,
	request *wirev1.RunRequest,
) (*wirev1.RunResponse, error) {
	operation, cancel, err := s.operationContext(ctx, request.GetConnectionId())
	if err != nil {
		return nil, err
	}
	defer cancel()

	parameters, err := decodeParameters(request.GetParameters())
	if err != nil {
		return nil, rpcError(&core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: err.Error()})
	}

	snapshot, err := s.executor.Run(operation, core.RunInput{
		Source: api.Source{
			Name:    request.GetSource().GetName(),
			Content: request.GetSource().GetContent(),
		},
		Parameters:        parameters,
		OutputContentType: request.GetOutputContentType(),
	})
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := execution(snapshot)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.RunResponse{Execution: converted}, nil
}
