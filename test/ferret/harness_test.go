// Package ferret_test validates the native Ferret adapter through Wire's public
// API over real gRPC. The nested module keeps native dependencies out of Wire.
package ferret_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/MontFerret/api"
	"github.com/MontFerret/ferret/v2/pkg/engine"
	ferretuapi "github.com/MontFerret/ferret/v2/uapi"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
)

const testTimeout = 10 * time.Second

type harness struct {
	ctx         context.Context
	native      *engine.Engine
	hosted      api.Runtime
	runtime     api.Runtime
	server      *server.Server
	listener    *bufconn.Listener
	transport   *grpc.ClientConn
	serveResult chan error
	resources   []io.Closer
	runtimes    []api.Runtime
	wireClosed  bool
	wireErr     error
}

func newHarness(t *testing.T, options ...server.Option) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	h := &harness{ctx: ctx}

	var err error

	h.native, err = engine.New(engine.WithFSRoot(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := h.native.Close(); err != nil {
			t.Errorf("close native engine: %v", err)
		}
	})
	h.hosted = ferretuapi.Wrap(h.native)
	t.Cleanup(func() {
		if err := h.hosted.Close(); err != nil {
			t.Errorf("close hosted adapter: %v", err)
		}
	})
	// Register before setup so partial construction also releases its owners.
	t.Cleanup(func() {
		if err := h.closeWire(); err != nil {
			t.Errorf("close Wire: %v", err)
		}
	})

	h.server, err = server.NewServer(h.hosted, options...)
	if err != nil {
		t.Fatal(err)
	}

	h.listener = bufconn.Listen(1 << 20)
	h.serveResult = make(chan error, 1)
	go func() { h.serveResult <- h.server.Serve(h.ctx, h.listener) }()

	h.transport, err = grpc.NewClient(
		"passthrough:///wire-native-ferret",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return h.listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	h.runtime, err = h.openRuntime()
	if err != nil {
		t.Fatal(err)
	}

	return h
}

func (h *harness) openRuntime() (api.Runtime, error) {
	remote, err := client.New(h.ctx, h.transport)
	if err != nil {
		return nil, err
	}

	h.runtimes = append(h.runtimes, remote)

	return remote, nil
}

func (h *harness) own(resource io.Closer) {
	h.resources = append(h.resources, resource)
}

// closeWire leaves the hosted adapter and engine available to the ownership
// test. Ordinary parent Close is not a substitute for closing each descendant.
func (h *harness) closeWire() error {
	if h.wireClosed {
		return h.wireErr
	}

	h.wireClosed = true
	for i := len(h.resources) - 1; i >= 0; i-- {
		h.wireErr = errors.Join(h.wireErr, h.resources[i].Close())
	}

	for i := len(h.runtimes) - 1; i >= 0; i-- {
		h.wireErr = errors.Join(h.wireErr, h.runtimes[i].Close())
	}

	if h.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		h.wireErr = errors.Join(h.wireErr, h.server.Shutdown(ctx))
		cancel()
	}

	if h.transport != nil {
		h.wireErr = errors.Join(h.wireErr, h.transport.Close())
	}

	if h.listener != nil {
		h.wireErr = errors.Join(h.wireErr, h.listener.Close())
	}

	if h.serveResult != nil {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		select {
		case err := <-h.serveResult:
			h.wireErr = errors.Join(h.wireErr, err)
		case <-ctx.Done():
			h.wireErr = errors.Join(h.wireErr, ctx.Err())
		}
	}

	return h.wireErr
}
