package grpcserver

import (
	"context"

	"google.golang.org/protobuf/proto"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// ReadExecutionOutput exclusively delivers one hosted output. Watches remain
// non-owning lifecycle observers and never carry or consume payload bytes.
func (s *ExecutionService) ReadExecutionOutput(request *wirev1.ReadExecutionOutputRequest, stream wirev1.ExecutionService_ReadExecutionOutputServer) error {
	operation, resources, cancel, err := prepareOperation(stream.Context(), s.connections, request.GetConnectionId())
	if err != nil {
		return err
	}
	defer cancel()

	execution, err := resources.Execution(operation, core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}

	return deliverStream(operation, execution.Context(), s.maxOutbound, stream.Send, func(ctx context.Context, send func(*wirev1.ReadExecutionOutputResponse) error) error {
		writer, err := newOutputWriter(s.maxOutbound,
			func(frame *wirev1.OutputFrame) proto.Message {
				return &wirev1.ReadExecutionOutputResponse{Frame: frame}
			},
			func(frame *wirev1.OutputFrame) error { return send(&wirev1.ReadExecutionOutputResponse{Frame: frame}) })
		if err != nil {
			return rpcError(err)
		}

		err = execution.ReadOutput(ctx, writer.start, writer.consume)
		if !writer.header {
			return rpcError(err)
		}

		return writer.finish(err)
	})
}
