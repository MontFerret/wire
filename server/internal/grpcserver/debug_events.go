package grpcserver

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func (s *Server) WatchDebug(request *wirev1.WatchDebugRequest, stream wirev1.DebugService_WatchDebugServer) error {
	operation, cancel, err := s.operationContext(stream.Context(), request.GetConnectionId())
	if err != nil {
		return err
	}

	defer cancel()

	session, err := s.debugger.Session(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}

	subscription, err := session.Watch()
	if err != nil {
		return rpcError(err)
	}

	defer subscription.Cancel()

	if subscription.Current.Sequence > 0 {
		converted, err := debugEvent(session.ID(), subscription.Current)
		if err != nil {
			return rpcError(err)
		}

		if err := stream.Send(converted); err != nil {
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

			converted, err := debugEvent(session.ID(), event)
			if err != nil {
				return rpcError(err)
			}

			if err := stream.Send(converted); err != nil {
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
