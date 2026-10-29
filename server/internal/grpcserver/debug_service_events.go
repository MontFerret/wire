package grpcserver

import (
	"bytes"
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/server/internal/core"
)

// WatchDebug sends the current snapshot followed by ordered debugger transitions.
// Stream completion releases the bounded subscription.
func (s *DebugService) WatchDebug(request *wirev1.WatchDebugRequest, stream wirev1.DebugService_WatchDebugServer) error {
	operation, resources, cancel, err := prepareOperation(stream.Context(), s.connections, request.GetConnectionId())
	if err != nil {
		return err
	}

	defer cancel()

	session, err := resources.DebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}

	subscription, err := session.Watch()
	if err != nil {
		return rpcError(err)
	}

	defer subscription.Cancel()

	return deliverStream(operation, session.DeliveryContext(), s.maxOutbound, stream.Send,
		func(operation context.Context, send func(*wirev1.WatchDebugResponse) error) error {
			sendEvent := func(event wiredebugger.Event) error {
				converted, err := debugEvent(session.ID(), event)
				if err != nil {
					return rpcError(err)
				}

				if err := send(converted); err != nil {
					return err
				}

				content, semanticErr, err := debugEventContent(event)
				if err != nil {
					return rpcError(err)
				}

				if content == nil {
					return nil
				}

				writer, err := newOutputWriter(s.maxOutbound,
					func(frame *wirev1.OutputFrame) proto.Message { return &wirev1.WatchDebugResponse{OutputFrame: frame} },
					func(frame *wirev1.OutputFrame) error { return send(&wirev1.WatchDebugResponse{OutputFrame: frame}) })
				if err != nil {
					return deliveryRPCError(err)
				}

				return writer.content(operation, content, semanticErr)
			}

			if subscription.Current.Sequence > 0 {
				if err := sendEvent(subscription.Current); err != nil {
					return err
				}
			}

			eventsChannel := subscription.Events
			errorsChannel := subscription.Errors

			for {
				select {
				case <-operation.Done():
					return rpcError(operation.Err())
				case event, ok := <-eventsChannel:
					if !ok {
						return subscriptionError(errorsChannel)
					}

					if err := sendEvent(event); err != nil {
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
		})
}

func debugEventContent(event wiredebugger.Event) (*api.Content, error, error) {
	content := event.Snapshot.Output
	var semanticErr error

	if event.Snapshot.Failure != nil {
		semanticErr = event.Snapshot.Failure
	}

	if result := event.Snapshot.CommandResult; result != nil {
		semanticErr = errors.Join(semanticErr, result.Error)
		if result.Event != nil {
			semanticErr = errors.Join(semanticErr, result.Event.Error)

			other := result.Event.Output
			if content != nil && other != nil && (content.Metadata != other.Metadata || !bytes.Equal(content.Data, other.Data)) {
				return nil, nil, runtimeConversionError("debug snapshot output disagrees with its command result")
			}

			if content == nil {
				content = other
			}
		}
	}

	return content, semanticErr, nil
}
