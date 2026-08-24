package client

import (
	"context"
	"errors"
	"io"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	runtimeRPCStub struct {
		closeRequest *wirev1.CloseConnectionRequest
		closeErr     error
	}

	connectRPCStream struct {
		response *wirev1.ConnectResponse
		err      error
	}
)

func (s *runtimeRPCStub) Connect(
	context.Context,
	*wirev1.ConnectRequest,
	...grpc.CallOption,
) (grpc.ServerStreamingClient[wirev1.ConnectResponse], error) {
	return nil, errors.New("unexpected Connect call")
}

func (s *runtimeRPCStub) CloseConnection(
	_ context.Context,
	request *wirev1.CloseConnectionRequest,
	_ ...grpc.CallOption,
) (*wirev1.CloseConnectionResponse, error) {
	s.closeRequest = request

	return &wirev1.CloseConnectionResponse{}, s.closeErr
}

func (s *connectRPCStream) Recv() (*wirev1.ConnectResponse, error) {
	response := s.response
	err := s.err
	s.response = nil
	s.err = io.EOF

	return response, err
}

func (s *connectRPCStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *connectRPCStream) Trailer() metadata.MD {
	return nil
}

func (s *connectRPCStream) CloseSend() error {
	return nil
}

func (s *connectRPCStream) Context() context.Context {
	return context.Background()
}

func (s *connectRPCStream) SendMsg(any) error {
	return nil
}

func (s *connectRPCStream) RecvMsg(any) error {
	return nil
}

func TestSessionOwnsConnectionProtocol(t *testing.T) {
	rpc := &runtimeRPCStub{}
	cancelled := false
	session := &session{
		rpc:           rpc,
		id:            "connection",
		connectStream: &connectRPCStream{err: io.EOF},
		connectCancel: func() { cancelled = true },
	}

	if err := session.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rpc.closeRequest.GetConnectionId().GetValue() != "connection" {
		t.Fatalf("unexpected close request: %#v", rpc.closeRequest)
	}
	if err := session.monitor(); err != nil {
		t.Fatalf("EOF did not settle the session: %v", err)
	}

	session.cancel()
	if !cancelled {
		t.Fatal("session did not cancel its Connect stream")
	}
}

func TestSessionRejectsUnexpectedConnectResponse(t *testing.T) {
	session := &session{connectStream: &connectRPCStream{response: &wirev1.ConnectResponse{}}}

	err := session.monitor()
	if err == nil || err.Error() != "Wire server returned an unexpected Connect response" {
		t.Fatalf("unexpected monitor result: %v", err)
	}
}
