package server

import gooptions "github.com/ziflex/go-options"

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

// DefaultLimits returns the secure finite limits used by New.
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
	values := map[string]int{
		"max connections":                   limits.MaxConnections,
		"max plans per connection":          limits.MaxPlansPerConnection,
		"max sessions per connection":       limits.MaxSessionsPerConnection,
		"max executions per connection":     limits.MaxExecutionsPerConnection,
		"max debug sessions per connection": limits.MaxDebugSessionsPerConnection,
		"max watchers per resource":         limits.MaxWatchersPerResource,
		"max breakpoints per debug session": limits.MaxBreakpointsPerDebugSession,
		"max inbound message bytes":         limits.MaxInboundMessageBytes,
		"max outbound message bytes":        limits.MaxOutboundMessageBytes,
	}

	return gooptions.MapValues[map[string]int](gooptions.Positive[int]())(values)
}
