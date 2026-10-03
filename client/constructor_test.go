package client

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestFromFailureReturnsNilRuntimeAndClosesHandshake(t *testing.T) {
	for _, test := range []struct {
		name      string
		handshake *wirev1.ConnectResponse
		err       error
	}{
		{name: "RPC failure", err: status.Error(codes.Unavailable, "handshake unavailable")},
		{name: "empty handshake", handshake: &wirev1.ConnectResponse{}},
		{name: "missing ID", handshake: &wirev1.ConnectResponse{RuntimeVersion: []byte{}, Protocol: &wirev1.ProtocolInfo{Name: "ferret.wire", Version: "v1"}}},
		{name: "missing protocol", handshake: &wirev1.ConnectResponse{RuntimeVersion: []byte{}, ConnectionId: &wirev1.ConnectionId{Value: "connection"}}},
		{name: "missing name", handshake: &wirev1.ConnectResponse{RuntimeVersion: []byte{}, ConnectionId: &wirev1.ConnectionId{Value: "connection"}, Protocol: &wirev1.ProtocolInfo{Version: "v1"}}},
		{name: "missing protocol version", handshake: &wirev1.ConnectResponse{RuntimeVersion: []byte{}, ConnectionId: &wirev1.ConnectionId{Value: "connection"}, Protocol: &wirev1.ProtocolInfo{Name: "ferret.wire"}}},
		{name: "missing runtime version", handshake: &wirev1.ConnectResponse{ConnectionId: &wirev1.ConnectionId{Value: "connection"}, Protocol: &wirev1.ProtocolInfo{Name: "ferret.wire", Version: "v1"}, RuntimeIdentity: &wirev1.RuntimeIdentity{Name: "host", Version: "host-7.1"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ended := make(chan struct{})
			server := &clientTestServer{handshake: test.handshake, connectErr: test.err, connectDone: ended}
			connection := startClientTestServer(t, server)
			ctx := testClientContext(t)

			remote, err := From(ctx, connection)
			if err == nil || remote != nil {
				t.Fatalf("From returned %v, %v; want nil runtime and an error", remote, err)
			}

			if test.err == nil && err.Error() != "Wire server returned an invalid Connect handshake" {
				t.Fatalf("malformed handshake used a different failure path: %v", err)
			}

			if test.err != nil {
				var decoded *Error
				if !errors.As(err, &decoded) || status.Code(err) != codes.Unavailable {
					t.Fatalf("constructor lost RPC failure: %v", err)
				}
			}

			select {
			case <-ended:
			case <-ctx.Done():
				t.Fatal("failed constructor retained its Connect stream")
			}
		})
	}
}

func TestFromBorrowsTransportAndDetachesConstructionContext(t *testing.T) {
	server := &clientTestServer{watchScripts: []executionWatchScript{{events: []*wirev1.WatchExecutionResponse{
		executionCompletedEvent("execution-1", "text/plain", []byte("done")),
	}}}}
	connection := startClientTestServer(t, server)
	ctx, cancel := context.WithCancel(testClientContext(t))
	defer cancel()

	remote, err := From(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := remote.Close(); err != nil {
			t.Error(err)
		}
	})
	cancel()

	output, err := remote.Run(testClientContext(t), api.NewAnonymousSource("RETURN 1"))
	if err != nil || string(output.Content) != "done" {
		t.Fatalf("construction cancellation affected runtime: %v, %v", output, err)
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	other, err := From(testClientContext(t), connection)
	if err != nil {
		t.Fatalf("runtime Close affected borrowed transport: %v", err)
	}

	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
}
