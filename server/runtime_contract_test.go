package server_test

import (
	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server"
)

// Server construction accepts the canonical runtime and server-owned identity.
var (
	_ func(server.RuntimeIdentity) server.Option                  = server.WithRuntimeIdentity
	_ func(api.Runtime, ...server.Option) (*server.Server, error) = server.NewServer
)
