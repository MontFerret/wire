package grpcserver

import "google.golang.org/grpc"

type registrationRecorder struct {
	services map[string]any
}

func (r *registrationRecorder) RegisterService(description *grpc.ServiceDesc, implementation any) {
	if _, exists := r.services[description.ServiceName]; exists {
		panic("duplicate service registration")
	}

	r.services[description.ServiceName] = implementation
}
