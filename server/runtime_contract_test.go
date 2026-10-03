package server_test

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server"
)

// Server construction accepts the canonical runtime and server-owned identity.
var (
	_ func(server.RuntimeIdentity) server.Option                               = server.WithRuntimeIdentity
	_ func(api.Runtime, ...server.Option) (*server.Server, error)              = server.New
	_ func(*server.Server, context.Context, string, ...server.RunOption) error = (*server.Server).Run
	_ func(time.Duration) server.RunOption                                     = server.WithShutdownTimeout
	_ func(credentials.TransportCredentials) server.Option                     = server.WithTransportCredentials
	_ func(...grpc.UnaryServerInterceptor) server.Option                       = server.WithUnaryInterceptors
	_ func(...grpc.StreamServerInterceptor) server.Option                      = server.WithStreamInterceptors
)
