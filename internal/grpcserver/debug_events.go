package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

func (s *Server) WatchDebug(request *wirev1.WatchDebugRequest, stream wirev1.DebugService_WatchDebugServer) error {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return err
	}

	subscription, err := connection.WatchDebug(core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}
	defer subscription.Cancel()

	if subscription.Current.Sequence > 0 {
		if err := stream.Send(debugEvent(subscription.Current)); err != nil {
			return err
		}
	}

	eventsChannel := subscription.Events
	errorsChannel := subscription.Errors
	for {
		select {
		case <-stream.Context().Done():
			return rpcError(stream.Context().Err())
		case event, ok := <-eventsChannel:
			if !ok {
				return subscriptionError(errorsChannel)
			}

			if err := stream.Send(debugEvent(event)); err != nil {
				return err
			}
		case watchErr, ok := <-errorsChannel:
			if ok && watchErr != nil {
				return rpcError(watchErr)
			}

			if !ok {
				errorsChannel = nil
			}
		}
	}
}
