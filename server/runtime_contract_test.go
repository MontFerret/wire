package server_test

import (
	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server"
)

// Check exact type identity as well as the public constructor signature.
var (
	_ func() server.Runtime                                          = (func() api.Runtime)(nil)
	_ func(server.Runtime, ...server.Option) (*server.Server, error) = server.NewServer
)
