package grpcserver

import (
	"reflect"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
)

func TestServerRegistersDedicatedProtocolServices(t *testing.T) {
	server := New(nil, Handshake{}, nil)
	registrar := &registrationRecorder{services: make(map[string]any)}
	server.Register(registrar)

	expected := map[string]reflect.Type{
		wirev1.RuntimeService_ServiceDesc.ServiceName:   reflect.TypeFor[*RuntimeService](),
		wirev1.PlanService_ServiceDesc.ServiceName:      reflect.TypeFor[*PlanService](),
		wirev1.SessionService_ServiceDesc.ServiceName:   reflect.TypeFor[*SessionService](),
		wirev1.ExecutionService_ServiceDesc.ServiceName: reflect.TypeFor[*ExecutionService](),
		wirev1.DebugService_ServiceDesc.ServiceName:     reflect.TypeFor[*DebugService](),
	}
	if len(registrar.services) != len(expected) {
		t.Fatalf("registered %d services, want %d", len(registrar.services), len(expected))
	}

	for name, implementation := range registrar.services {
		if reflect.TypeOf(implementation) != expected[name] {
			t.Errorf("%s registered %T, want %v", name, implementation, expected[name])
		}
	}

	// Real registration checks the generated interfaces and embedded bases too.
	transport := grpc.NewServer()
	t.Cleanup(transport.Stop)
	server.Register(transport)
	for name := range expected {
		if _, exists := transport.GetServiceInfo()[name]; !exists {
			t.Errorf("service %s is unavailable on the gRPC server", name)
		}
	}
}
