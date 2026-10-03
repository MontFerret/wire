package server

import (
	"errors"
	"reflect"

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
	Option interface {
		apply(*config) error
	}

	serverOptionFunc func(*config) error

	config struct {
		runtimeIdentity RuntimeIdentity
		limits          Limits
		credentials     credentials.TransportCredentials
		unary           []grpc.UnaryServerInterceptor
		stream          []grpc.StreamServerInterceptor
	}
)

func (option serverOptionFunc) apply(cfg *config) error {
	return option(cfg)
}

// WithTransportCredentials sets the host's gRPC transport policy for both Run
// and Serve. New rejects nil credentials. Repeated options use the last value.
// Without credentials, Wire provides no transport encryption or authentication.
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return serverOptionFunc(func(cfg *config) error {
		if creds == nil {
			return errors.New("transport credentials must not be nil")
		}

		value := reflect.ValueOf(creds)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return errors.New("transport credentials must not be nil")
			}
		}

		cfg.credentials = creds

		return nil
	})
}

// WithUnaryInterceptors appends host middleware for every unary RPC, inside
// Wire's sanitized recovery boundary. Nil entries are rejected by New.
// The interceptor list is copied; middleware itself must support concurrent calls.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	captured := append([]grpc.UnaryServerInterceptor(nil), interceptors...)

	return serverOptionFunc(func(cfg *config) error {
		for _, interceptor := range captured {
			if interceptor == nil {
				return errors.New("unary interceptor must not be nil")
			}
		}

		cfg.unary = append(cfg.unary, captured...)

		return nil
	})
}

// WithStreamInterceptors appends host middleware for every streaming RPC,
// inside Wire's sanitized recovery boundary. Nil entries are rejected by New.
// The interceptor list is copied; middleware itself must support concurrent calls.
// Authentication here applies at stream establishment; later revocation is host policy.
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) Option {
	captured := append([]grpc.StreamServerInterceptor(nil), interceptors...)

	return serverOptionFunc(func(cfg *config) error {
		for _, interceptor := range captured {
			if interceptor == nil {
				return errors.New("stream interceptor must not be nil")
			}
		}

		cfg.stream = append(cfg.stream, captured...)

		return nil
	})
}

// WithRuntimeIdentity publishes optional host application identity during the
// Connect handshake. Name is required; Wire does not derive identity from the
// process or environment.
func WithRuntimeIdentity(identity RuntimeIdentity) Option {
	return serverOptionFunc(func(cfg *config) error {
		if identity.Name == "" {
			return errors.New("runtime identity name is required")
		}

		cfg.runtimeIdentity = identity

		return nil
	})
}

// WithLimits replaces the complete default limit set. New rejects
// the option when any resource or message limit is not positive.
func WithLimits(limits Limits) Option {
	return serverOptionFunc(func(cfg *config) error {
		if err := limits.validate(); err != nil {
			return err
		}

		cfg.limits = limits

		return nil
	})
}
