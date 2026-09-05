package server

import (
	"errors"

	"github.com/MontFerret/wire/pkg/execution"
)

type (
	// ServerOption configures a Server without transferring host ownership.
	ServerOption interface {
		apply(*serverOptions) error
	}

	serverOptionFunc func(*serverOptions) error

	serverOptions struct {
		runtimeIdentity execution.Identity
		limits          ServerLimits
	}
)

func (option serverOptionFunc) apply(options *serverOptions) error {
	return option(options)
}

// WithRuntimeIdentity publishes optional host application identity during the
// Connect handshake. Name is required; Wire does not derive identity from the
// process or environment.
func WithRuntimeIdentity(identity execution.Identity) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if identity.Name == "" {
			return errors.New("runtime identity name is required")
		}

		options.runtimeIdentity = identity

		return nil
	})
}

// WithServerLimits replaces the complete default limit set. NewServer rejects
// the option when any resource or message limit is not positive.
func WithServerLimits(limits ServerLimits) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if err := limits.validate(); err != nil {
			return err
		}

		options.limits = limits

		return nil
	})
}
