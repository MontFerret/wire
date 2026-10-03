package server

import (
	gooptions "github.com/ziflex/go-options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type (
	// RuntimeIdentity is optional host-supplied handshake metadata. Wire does
	// not derive it from the runtime implementation or process environment.
	RuntimeIdentity struct {
		Name       string
		Version    string
		InstanceID string
	}

	// Option configures a Server without transferring host ownership.
	// New applies every non-nil option in registration order and joins failures.
	// Nil functions are rejected before server construction.
	// Factory validation failures expose gooptions.ValidationError through errors.As.
	Option func(*config) error

	config struct {
		runtimeIdentity RuntimeIdentity
		limits          Limits
		credentials     credentials.TransportCredentials
		unary           []grpc.UnaryServerInterceptor
		stream          []grpc.StreamServerInterceptor
	}
)

// WithTransportCredentials sets the host's gRPC transport policy for both Run
// and Serve. New rejects nil credentials. Repeated options use the last value.
// Without credentials, Wire provides no transport encryption or authentication.
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return gooptions.New(func(cfg *config, value credentials.TransportCredentials) {
		cfg.credentials = value
	}).
		Value(creds).
		Named("transport credentials").
		Validators(gooptions.NotNil[credentials.TransportCredentials]()).
		Build()
}

// WithUnaryInterceptors appends host middleware for every unary RPC, inside
// Wire's sanitized recovery boundary. Nil entries are rejected by New.
// Validation errors identify entries by relative index and omit the list value.
// The interceptor list is copied; middleware itself must support concurrent calls.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	captured := append([]grpc.UnaryServerInterceptor(nil), interceptors...)

	return gooptions.New(func(cfg *config, value []grpc.UnaryServerInterceptor) {
		cfg.unary = append(cfg.unary, value...)
	}).
		Value(captured).
		Named("unary interceptors").
		Validators(
			gooptions.SliceEach[[]grpc.UnaryServerInterceptor](gooptions.NotNil[grpc.UnaryServerInterceptor]()),
		).
		Build()
}

// WithStreamInterceptors appends host middleware for every streaming RPC,
// inside Wire's sanitized recovery boundary. Nil entries are rejected by New.
// Validation errors identify entries by relative index and omit the list value.
// The interceptor list is copied; middleware itself must support concurrent calls.
// Authentication here applies at stream establishment; later revocation is host policy.
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) Option {
	captured := append([]grpc.StreamServerInterceptor(nil), interceptors...)

	return gooptions.New(func(cfg *config, value []grpc.StreamServerInterceptor) {
		cfg.stream = append(cfg.stream, value...)
	}).
		Value(captured).
		Named("stream interceptors").
		Validators(
			gooptions.SliceEach[[]grpc.StreamServerInterceptor](gooptions.NotNil[grpc.StreamServerInterceptor]()),
		).
		Build()
}

// WithRuntimeIdentity publishes optional host application identity during the
// Connect handshake. Name is required; Wire does not derive identity from the
// process or environment.
func WithRuntimeIdentity(identity RuntimeIdentity) Option {
	return gooptions.New(func(cfg *config, value RuntimeIdentity) {
		cfg.runtimeIdentity = value
	}).
		Value(identity).
		Named("runtime identity name").
		Validators(
			gooptions.Check(func(value RuntimeIdentity) error {
				return gooptions.NotEmpty[string]()(value.Name)
			}),
		).
		Build()
}

// WithLimits replaces the complete default limit set. New rejects
// the option when any resource or message limit is not positive, reporting
// every invalid field without applying a partial replacement.
// Validation errors use relative field-key labels; their order is unspecified.
func WithLimits(limits Limits) Option {
	return gooptions.New(func(cfg *config, value Limits) {
		cfg.limits = value
	}).
		Value(limits).
		Named("limits").
		Validators(
			gooptions.Check(Limits.validate),
		).
		Build()
}
