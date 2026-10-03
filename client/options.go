package client

import (
	"crypto/tls"
	"errors"

	gooptions "github.com/ziflex/go-options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	// Option configures New's transport without acquiring ownership of credential
	// providers. New applies non-nil options once in order and joins failures.
	// Invalid options prevent channel creation, even when followed by an override.
	Option func(*config) error

	config struct {
		transport credentials.TransportCredentials
		perRPC    []credentials.PerRPCCredentials
		insecure  bool
	}
)

// WithTransportCredentials replaces New's default TLS credentials. The last
// valid value wins. Nil credentials fail construction, and combining this option
// with WithInsecure fails in either order. The provider retains its own policy
// and external resources; Wire neither mutates nor closes it.
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return gooptions.New(func(cfg *config, value credentials.TransportCredentials) {
		cfg.transport = value
	}).
		Value(creds).
		Named("transport credentials").
		Validators(gooptions.NotNil[credentials.TransportCredentials]()).
		Build()
}

// WithPerRPCCredentials appends a provider in registration order for every RPC,
// including Connect, subsequent unary and streaming calls, and detached cleanup.
// Providers may refresh credentials; gRPC enforces RequireTransportSecurity.
// Nil providers fail construction. Providers must support concurrent RPCs.
func WithPerRPCCredentials(creds credentials.PerRPCCredentials) Option {
	return gooptions.New(func(cfg *config, value credentials.PerRPCCredentials) {
		cfg.perRPC = append(cfg.perRPC, value)
	}).
		Value(creds).
		Named("per-RPC credentials").
		Validators(gooptions.NotNil[credentials.PerRPCCredentials]()).
		Build()
}

// WithInsecure selects plaintext, disabling transport encryption and peer
// authentication. It does not select TLS without certificate verification.
// Repetition is idempotent; explicit transport credentials conflict with it.
func WithInsecure() Option {
	return gooptions.New(func(cfg *config, value bool) {
		cfg.insecure = value
	}).
		Value(true).
		Build()
}

func configure(options []Option) (config, error) {
	var configured config
	var errs []error

	for _, option := range options {
		if option == nil {
			errs = append(errs, errors.New("client option must not be nil"))

			continue
		}

		errs = append(errs, option(&configured))
	}

	if configured.insecure && configured.transport != nil {
		errs = append(errs, errors.New("WithInsecure conflicts with explicit transport credentials"))
	}

	return configured, errors.Join(errs...)
}

func (c config) dialOptions() []grpc.DialOption {
	transport := c.transport

	if c.insecure {
		transport = insecure.NewCredentials()
	} else if transport == nil {
		transport = credentials.NewTLS(&tls.Config{})
	}

	options := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
	for _, provider := range c.perRPC {
		options = append(options, grpc.WithPerRPCCredentials(provider))
	}

	return options
}
