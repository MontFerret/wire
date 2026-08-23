package grpcserver

import (
	"context"

	"github.com/MontFerret/wire/internal/core"
	"google.golang.org/grpc"
)

func UnaryRecoveryInterceptor(
	ctx context.Context,
	request any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (response any, err error) {
	defer func() {
		if recover() != nil {
			response = nil
			err = rpcError(&core.DomainError{Category: core.ErrorInternal, Message: "internal runtime failure"})
		}
	}()
	return handler(ctx, request)
}

func StreamRecoveryInterceptor(
	server any,
	stream grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	defer func() {
		if recover() != nil {
			err = rpcError(&core.DomainError{Category: core.ErrorInternal, Message: "internal runtime failure"})
		}
	}()
	return handler(server, stream)
}
