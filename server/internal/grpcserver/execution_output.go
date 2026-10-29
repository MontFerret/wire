package grpcserver

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	"github.com/MontFerret/wire/server/internal/core"
)

// ReadExecutionOutput exclusively delivers one hosted output. Watches remain
// non-owning lifecycle observers and never carry or consume payload bytes.
func (s *ExecutionService) ReadExecutionOutput(request *wirev1.ReadExecutionOutputRequest, stream wirev1.ExecutionService_ReadExecutionOutputServer) error {
	connection, err := s.connections.Get(core.ConnectionID(request.GetConnectionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}

	if err := connection.Context().Err(); err != nil {
		return rpcError(err)
	}

	execution, err := connection.Resources().Execution(stream.Context(), core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}

	size, err := s.outputChunkSize()
	if err != nil {
		return deliveryRPCError(err)
	}

	delivery := newExecutionDelivery(stream.Context(), execution.Context(), s.maxOutbound)
	defer delivery.cancel(context.Canceled)
	go delivery.run(stream.Send)
	writer := &outputWriter{size: size, send: delivery.send}

	err = execution.ReadOutput(delivery.ctx, writer.start, writer.consume)
	if delivery.ctx.Err() != nil || execution.Context().Err() != nil {
		// Callbacks and cleanup can still be running. Only the execution owner
		// may touch writer state until settlement on this path.
		return delivery.abort(errors.Join(err, delivery.ctx.Err(), execution.Context().Err()))
	}

	if !writer.header {
		return deliveryRPCError(err)
	}

	if err := writer.finish(err); err != nil {
		return err
	}

	return delivery.wait()
}

// The outbound limit is immutable for this service; each transfer reuses its
// envelope budget, including a failure when even one payload byte cannot fit.
func (s *ExecutionService) outputChunkSize() (int, error) {
	s.chunkOnce.Do(func() {
		s.chunkSize, s.chunkErr = outputstream.MaxChunk(s.maxOutbound, func(frame *wirev1.OutputFrame) proto.Message {
			return &wirev1.ReadExecutionOutputResponse{Frame: frame}
		})

		if s.chunkErr != nil {
			s.chunkErr = outboundLimitError("outbound message limit cannot fit an output chunk")
		}
	})

	return s.chunkSize, s.chunkErr
}
