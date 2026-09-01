package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/lifecycle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type lifecycleServer struct {
	wirev1.UnimplementedRuntimeServiceServer
	wirev1.UnimplementedExecutionServiceServer

	disconnect   bool
	closeEntered chan struct{}
	allowClose   chan struct{}
	closeOnce    sync.Once
}

func (s *lifecycleServer) Connect(_ *wirev1.ConnectRequest, stream wirev1.RuntimeService_ConnectServer) error {
	err := stream.Send(&wirev1.ConnectResponse{
		ConnectionId: &wirev1.ConnectionId{Value: "connection"},
		Protocol:     &wirev1.ProtocolInfo{Name: "ferret.wire", Version: "v1"},
	})
	if err != nil || s.disconnect {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (s *lifecycleServer) CloseConnection(context.Context, *wirev1.CloseConnectionRequest) (*wirev1.CloseConnectionResponse, error) {
	if s.disconnect {
		detail := &wirev1.ErrorDetail{
			Category: wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND,
		}
		withDetails, err := status.New(codes.NotFound, "resource not found").WithDetails(detail)
		if err != nil {
			return nil, err
		}

		return nil, withDetails.Err()
	}

	s.closeOnce.Do(func() { close(s.closeEntered) })
	<-s.allowClose

	return &wirev1.CloseConnectionResponse{}, nil
}

func (s *lifecycleServer) WatchExecution(_ *wirev1.WatchExecutionRequest, stream wirev1.ExecutionService_WatchExecutionServer) error {
	err := stream.Send(&wirev1.WatchExecutionResponse{
		Sequence: 1,
		Execution: &wirev1.Execution{
			Id:    &wirev1.ExecutionId{Value: "execution"},
			State: wirev1.ExecutionState_EXECUTION_STATE_RUNNING,
		},
	})
	if err != nil {
		return err
	}

	<-stream.Context().Done()

	return stream.Context().Err()
}

func TestCloseAfterServerDisconnectTreatsMissingConnectionAsSettled(t *testing.T) {
	server := &lifecycleServer{disconnect: true}
	connection := startLifecycleServer(t, server)
	client, err := New(testClientContext(t), connection)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.streamDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Connect stream did not disconnect")
	}

	if err := client.Close(testClientContext(t)); err != nil {
		t.Fatalf("close-after-disconnect did not settle successfully: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.Close(cancelled); err != nil {
		t.Fatalf("completed close did not retain its result: %v", err)
	}
}

func TestCloseRejectsNewOperationsAndCancelsFacadeWatchers(t *testing.T) {
	server := &lifecycleServer{closeEntered: make(chan struct{}), allowClose: make(chan struct{})}
	connection := startLifecycleServer(t, server)
	client, err := New(testClientContext(t), connection)
	if err != nil {
		t.Fatal(err)
	}
	plan := &Plan{client: client, id: "plan", close: &lifecycle.Close{}}
	execution := &Execution{client: client, plan: plan, id: "execution", close: &lifecycle.Close{}}
	events, err := execution.Watch(testClientContext(t))
	if err != nil {
		t.Fatal(err)
	}

	if event, err := events.Recv(); err != nil || event.Snapshot.State != ExecutionRunning {
		t.Fatalf("watcher did not attach: %#v, %v", event, err)
	}

	closeResult := make(chan error, 1)
	closeCtx := testClientContext(t)
	go func() { closeResult <- client.Close(closeCtx) }()
	select {
	case <-server.closeEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("client close did not reach the server")
	}

	if _, err := client.Compile(context.Background(), api.Source{Content: "RETURN 1"}, CompileOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("client accepted a new operation after close started: %v", err)
	}
	close(server.allowClose)
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}

	if _, err := events.Recv(); err == nil {
		t.Fatal("facade watcher survived logical client close")
	} else {
		var wireErr *Error
		if !errors.As(err, &wireErr) || wireErr.Category != ErrorCancelled {
			t.Fatalf("unexpected watcher cancellation: %v", err)
		}
	}
}

func startLifecycleServer(t *testing.T, implementation *lifecycleServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	wirev1.RegisterRuntimeServiceServer(server, implementation)
	wirev1.RegisterExecutionServiceServer(server, implementation)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	connection, err := grpc.NewClient(
		"passthrough:///client-lifecycle-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		server.Stop()
		if err := connection.Close(); err != nil {
			t.Errorf("transport cleanup failed: %v", err)
		}

		if err := listener.Close(); err != nil {
			t.Errorf("listener cleanup failed: %v", err)
		}
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("server cleanup failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("server cleanup did not settle")
		}
	})

	return connection
}

func testClientContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	return ctx
}
