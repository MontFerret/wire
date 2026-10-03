package securityfixture

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Token supplies a complete authorization header on every RPC over TLS.
type Token string

// GetRequestMetadata attaches the authorization header to each RPC.
func (t Token) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if t == "" {
		return nil, nil
	}

	return map[string]string{"authorization": string(t)}, nil
}

// RequireTransportSecurity prevents tokens being sent over plaintext transport.
func (Token) RequireTransportSecurity() bool { return true }

// Authenticate validates the fixture's host-supplied token before Wire handlers run.
func Authenticate(ctx context.Context) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || values[0] != "Bearer test-token" {
		return status.Error(codes.Unauthenticated, "authentication required")
	}

	return nil
}

// Unary authenticates every unary request in the test host.
func Unary(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if err := Authenticate(ctx); err != nil {
		return nil, err
	}

	return next(ctx, request)
}

// Stream authenticates every stream at establishment in the test host.
func Stream(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	if err := Authenticate(stream.Context()); err != nil {
		return err
	}

	return next(server, stream)
}
