package server

import (
	"errors"

	"github.com/MontFerret/wire/pkg/execution"
)

type (
	// Option configures a Server without transferring host ownership.
	Option interface {
		apply(*config) error
	}

	serverOptionFunc func(*config) error

	config struct {
		runtimeIdentity execution.Identity
		limits          Limits
	}
)

func (option serverOptionFunc) apply(cfg *config) error {
	return option(cfg)
}

// WithRuntimeIdentity publishes optional host application identity during the
// Connect handshake. Name is required; Wire does not derive identity from the
// process or environment.
func WithRuntimeIdentity(identity execution.Identity) Option {
	return serverOptionFunc(func(cfg *config) error {
		if identity.Name == "" {
			return errors.New("runtime identity name is required")
		}

		cfg.runtimeIdentity = identity

		return nil
	})
}

// WithLimits replaces the complete default limit set. NewServer rejects
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
