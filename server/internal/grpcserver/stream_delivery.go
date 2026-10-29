package grpcserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/MontFerret/wire/server/internal/core"
)

// deliverStream separates cancellation from a potentially blocked gRPC Send.
// At most one detached message is being sent and one is held by the producer.
// Returning the handler cancels gRPC's stream and unblocks its sender. Producers
// retain their resource owner until cleanup settles, even if the RPC has ended.
func deliverStream[T proto.Message](ctx, lifetime context.Context, limit int, send func(T) error, produce func(context.Context, func(T) error) error) error {
	operation, cancel := core.OperationContext(ctx, lifetime)
	defer cancel()

	messages := make(chan T)
	produced := make(chan error, 1)
	sent := make(chan error, 1)

	go func() {
		var result error
		defer func() {
			if recover() != nil {
				result = status.Error(codes.Internal, "Wire stream producer panicked")
			}

			produced <- result
			close(messages)
		}()

		result = produce(operation, func(message T) error {
			if proto.Size(message) > limit {
				return status.Error(codes.ResourceExhausted, "outbound message limit exceeded")
			}

			select {
			case messages <- message:
				return nil
			case <-operation.Done():
				return operation.Err()
			}
		})
	}()

	go func() {
		for {
			select {
			case <-operation.Done():
				sent <- operation.Err()

				return
			case message, ok := <-messages:
				if !ok {
					sent <- <-produced

					return
				}

				if err := send(message); err != nil {
					sent <- err

					return
				}
			}
		}
	}()

	select {
	case err := <-sent:
		return err
	case <-operation.Done():
		return rpcError(errors.Join(operation.Err(), context.Cause(operation)))
	}
}
