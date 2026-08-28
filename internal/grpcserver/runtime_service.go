package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

func (s *Server) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	connection, err := s.host.OpenConnection()
	if err != nil {
		return rpcError(err)
	}
	defer func() {
		_ = s.host.CloseConnection(context.Background(), connection.ID())
	}()

	response := &wirev1.ConnectResponse{Opened: &wirev1.ConnectionOpened{
		ConnectionId: &wirev1.ConnectionId{Value: string(connection.ID())},
		RuntimeInfo:  runtimeInfo(s.host.Info()),
	}}
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
	err := s.host.CloseConnection(ctx, core.ConnectionID(request.GetConnectionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.CloseConnectionResponse{}, nil
}
