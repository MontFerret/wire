package server

import (
	"fmt"
)

// Limits bounds all client-controlled resource and message classes.
// A custom value replaces the complete default set and every field must be
// positive.
type Limits struct {
	MaxConnections                int
	MaxPlansPerConnection         int
	MaxSessionsPerConnection      int
	MaxExecutionsPerConnection    int
	MaxDebugSessionsPerConnection int
	MaxWatchersPerResource        int
	MaxBreakpointsPerDebugSession int
	MaxInboundMessageBytes        int
	MaxOutboundMessageBytes       int
}

// DefaultLimits returns the secure finite limits used by NewServer.
func DefaultLimits() Limits {
	return Limits{
		MaxConnections:                64,
		MaxPlansPerConnection:         128,
		MaxSessionsPerConnection:      128,
		MaxExecutionsPerConnection:    128,
		MaxDebugSessionsPerConnection: 32,
		MaxWatchersPerResource:        8,
		MaxBreakpointsPerDebugSession: 256,
		MaxInboundMessageBytes:        4 << 20,
		MaxOutboundMessageBytes:       4 << 20,
	}
}

func (limits Limits) validate() error {
	values := []struct {
		name  string
		value int
	}{
		{name: "max connections", value: limits.MaxConnections},
		{name: "max plans per connection", value: limits.MaxPlansPerConnection},
		{name: "max sessions per connection", value: limits.MaxSessionsPerConnection},
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
