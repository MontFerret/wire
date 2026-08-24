package wire

import (
	"errors"
	"fmt"
)

type (
	// RuntimeIdentity identifies the host application exposed by a Server.
	RuntimeIdentity struct {
		Name       string
		Version    string
		InstanceID string
	}

	// ServerLimits bounds all client-controlled resource and message classes.
	// A custom value replaces the complete default set and every field must be
	// positive.
	ServerLimits struct {
		MaxConnections                int
		MaxPlansPerConnection         int
		MaxExecutionsPerConnection    int
		MaxDebugSessionsPerConnection int
		MaxWatchersPerResource        int
		MaxBreakpointsPerDebugSession int
		MaxInboundMessageBytes        int
		MaxOutboundMessageBytes       int
	}

	// ServerOption configures a Server without transferring host ownership.
	ServerOption interface {
		apply(*serverOptions) error
	}

	serverOptionFunc func(*serverOptions) error

	serverOptions struct {
		runtimeIdentity RuntimeIdentity
		limits          ServerLimits
	}
)

func (option serverOptionFunc) apply(options *serverOptions) error {
	return option(options)
}

// WithRuntimeIdentity publishes optional host application identity during the
// Connect handshake. Name is required; Wire does not derive identity from the
// process or environment.
func WithRuntimeIdentity(identity RuntimeIdentity) ServerOption {
	return serverOptionFunc(func(options *serverOptions) error {
		if identity.Name == "" {
			return errors.New("runtime identity name is required")
		}

		options.runtimeIdentity = identity

		return nil
	})
}

// DefaultServerLimits returns the secure finite limits used by NewServer.
func DefaultServerLimits() ServerLimits {
	return ServerLimits{
		MaxConnections:                64,
		MaxPlansPerConnection:         128,
		MaxExecutionsPerConnection:    128,
		MaxDebugSessionsPerConnection: 32,
		MaxWatchersPerResource:        8,
		MaxBreakpointsPerDebugSession: 256,
		MaxInboundMessageBytes:        4 << 20,
		MaxOutboundMessageBytes:       4 << 20,
	}
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

func (limits ServerLimits) validate() error {
	values := []struct {
		name  string
		value int
	}{
		{name: "max connections", value: limits.MaxConnections},
		{name: "max plans per connection", value: limits.MaxPlansPerConnection},
		{name: "max executions per connection", value: limits.MaxExecutionsPerConnection},
		{name: "max debug sessions per connection", value: limits.MaxDebugSessionsPerConnection},
		{name: "max watchers per resource", value: limits.MaxWatchersPerResource},
		{name: "max breakpoints per debug session", value: limits.MaxBreakpointsPerDebugSession},
		{name: "max inbound message bytes", value: limits.MaxInboundMessageBytes},
		{name: "max outbound message bytes", value: limits.MaxOutboundMessageBytes},
	}

	for _, value := range values {
		if value.value <= 0 {
			return fmt.Errorf("%s must be positive", value.name)
		}
	}

	return nil
}
