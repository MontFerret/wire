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
	registry := core.NewConnectionRegistry(1, core.ResourceLimits{})
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
			operation, resources, cancel, err := prepareOperation(context.Background(), registry, test.id)
			if status.Code(err) != test.code || operation != nil || resources != nil || cancel != nil {
				t.Fatalf("context result = (%v, %v, %v), want %v", operation, cancel == nil, err, test.code)
			}
		})
	}
}

func TestOperationContextCombinesLifetimesAndPreservesValues(t *testing.T) {
	for _, lifetime := range []string{"request", "connection", "operation"} {
		t.Run(lifetime, func(t *testing.T) {
			registry := core.NewConnectionRegistry(1, core.ResourceLimits{})
			connection, err := registry.Open()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				if err := registry.Close(ctx); err != nil {
					t.Error(err)
				}
			})

			type contextKey struct{}
			request, cancelRequest := context.WithTimeout(context.WithValue(context.Background(), contextKey{}, "retained"), 5*time.Second)
			defer cancelRequest()
			id := &wirev1.ConnectionId{Value: string(connection.ID())}
			operation, resources, cancel, err := prepareOperation(request, registry, id)
			if err != nil {
				t.Fatal(err)
			}

			defer cancel()

			deadline, _ := request.Deadline()
			operationDeadline, present := operation.Deadline()
			if resources != connection.Resources() || operation.Value(contextKey{}) != "retained" || !present || !operationDeadline.Equal(deadline) {
				t.Fatal("operation lost its connection, request value, or deadline")
			}

			switch lifetime {
			case "request":
				cancelRequest()
			case "connection":
				if err := registry.CloseConnection(request, connection.ID()); err != nil {
					t.Fatal(err)
				}

				if _, _, _, err := prepareOperation(request, registry, id); status.Code(err) != codes.NotFound {
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
