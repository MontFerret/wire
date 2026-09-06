package grpcserver

import (
	"context"

	"google.golang.org/grpc"

	"github.com/MontFerret/wire/server/internal/core"
)

// UnaryRecoveryInterceptor replaces recovered handler panics with sanitized internal statuses.
func UnaryRecoveryInterceptor(
	ctx context.Context,
	request any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (response any, err error) {
	defer func() {
		if recover() != nil {
			response = nil
			err = rpcError(&core.DomainError{Kind: core.ErrorKindInternal, Message: "internal runtime failure"})
		}
	}()

	return handler(ctx, request)
}

// StreamRecoveryInterceptor replaces recovered stream-handler panics with sanitized internal statuses.
func StreamRecoveryInterceptor(
	server any,
	stream grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	defer func() {
		if recover() != nil {
			err = rpcError(&core.DomainError{Kind: core.ErrorKindInternal, Message: "internal runtime failure"})
		}
	}()

	return handler(server, stream)
}
