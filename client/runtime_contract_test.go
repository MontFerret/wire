package client_test

import (
	"context"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/result"
	"github.com/MontFerret/wire/client"
	"google.golang.org/grpc"
)

// Function types require identical parameter and result types, so these checks
// reject new defined types even when they satisfy the same interfaces.
var (
	_ func() client.Runtime = (func() api.Runtime)(nil)
	_ func() client.Session = (func() api.Session)(nil)
	_ func() client.Output  = (func() api.Output)(nil)
	_ func() client.Output  = (func() result.Output)(nil)

	_ func(context.Context, grpc.ClientConnInterface) (client.Runtime, error) = client.NewRuntime
)
