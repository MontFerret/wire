package grpcserver

import (
	"context"
	"errors"
	"testing"
	"time"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOperationContextRejectsInvalidAndUnknownConnections(t *testing.T) {
	factory := &operationContextFactory{connections: core.NewConnectionRegistry(1)}
	for _, test := range []struct {
		name string
		id   *wirev1.ConnectionId
		code codes.Code
	}{
		{name: "missing", code: codes.InvalidArgument},
		{name: "empty", id: &wirev1.ConnectionId{}, code: codes.InvalidArgument},
		{name: "malformed", id: &wirev1.ConnectionId{Value: "bad"}, code: codes.InvalidArgument},
		{name: "unknown", id: &wirev1.ConnectionId{Value: uuid.NewString()}, code: codes.NotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			operation, cancel, err := factory.New(context.Background(), test.id)
			if status.Code(err) != test.code || operation != nil || cancel != nil {
				t.Fatalf("context result = (%v, %v, %v), want %v", operation, cancel == nil, err, test.code)
			}
		})
	}
}

func TestOperationContextCombinesLifetimesAndPreservesValues(t *testing.T) {
	for _, lifetime := range []string{"request", "connection", "operation"} {
		t.Run(lifetime, func(t *testing.T) {
			registry := core.NewConnectionRegistry(1)
			connection := core.NewConnection()
			if err := registry.Register(connection); err != nil {
				t.Fatal(err)
			}

			lifecycle := core.NewLifecycle(registry, core.NewPlanRegistry(1), core.NewSessionRegistry(1), core.NewExecutionRegistry(1, 1), core.NewDebugSessionRegistry(1, 1, 1))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				if err := lifecycle.Close(ctx); err != nil {
					t.Error(err)
				}
			})

			type contextKey struct{}
			request, cancelRequest := context.WithTimeout(context.WithValue(context.Background(), contextKey{}, "retained"), 5*time.Second)
			defer cancelRequest()
			factory := &operationContextFactory{connections: registry}
			id := &wirev1.ConnectionId{Value: string(connection.ID())}
			operation, cancel, err := factory.New(request, id)
			if err != nil {
				t.Fatal(err)
			}

			defer cancel()

			deadline, _ := request.Deadline()
			operationDeadline, present := operation.Deadline()
			if operation.Connection() != connection || operation.Value(contextKey{}) != "retained" || !present || !operationDeadline.Equal(deadline) {
				t.Fatal("operation lost its connection, request value, or deadline")
			}

			switch lifetime {
			case "request":
				cancelRequest()
			case "connection":
				if err := lifecycle.CloseConnection(request, connection.ID()); err != nil {
					t.Fatal(err)
				}

				if _, _, err := factory.New(request, id); status.Code(err) != codes.NotFound {
					t.Fatalf("closed connection was resolved: %v", err)
				}
			case "operation":
				cancel()
			}

			select {
			case <-operation.Done():
				if !errors.Is(operation.Err(), context.Canceled) {
					t.Fatalf("cancellation was lost: %v", operation.Err())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("operation context did not cancel")
			}

			if lifetime != "connection" && connection.Context().Err() != nil {
				t.Fatal("request or operation cancellation closed the connection")
			}
		})
	}
}
