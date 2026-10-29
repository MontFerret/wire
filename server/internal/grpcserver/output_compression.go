package grpcserver

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// OutputCompressionInterceptor selects identity before host middleware can send
// headers. Exact protobuf budgets then also bound the transport payload size.
func OutputCompressionInterceptor(host any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	switch info.FullMethod {
	case wirev1.ExecutionService_ReadExecutionOutput_FullMethodName,
		wirev1.DebugService_RunCommand_FullMethodName,
		wirev1.DebugService_WatchDebug_FullMethodName:
		if err := grpc.SetSendCompressor(stream.Context(), encoding.Identity); err != nil {
			return rpcError(&core.DomainError{Kind: core.ErrorKindInternal})
		}
	}

	return next(host, stream)
}
