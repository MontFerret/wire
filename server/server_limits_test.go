package server_test

import (
	"testing"

	"github.com/MontFerret/wire/server"
)

func TestDefaultServerLimits(t *testing.T) {
	limits := server.DefaultLimits()
	if limits.MaxConnections != 64 ||
		limits.MaxPlansPerConnection != 128 ||
		limits.MaxSessionsPerConnection != 128 ||
		limits.MaxExecutionsPerConnection != 128 ||
		limits.MaxDebugSessionsPerConnection != 32 ||
		limits.MaxWatchersPerResource != 8 ||
		limits.MaxBreakpointsPerDebugSession != 256 ||
		limits.MaxInboundMessageBytes != 4<<20 ||
		limits.MaxOutboundMessageBytes != 4<<20 {
		t.Fatalf("unexpected default limits: %#v", limits)
	}
}

func TestServerLimitsRequireEveryValueToBePositive(t *testing.T) {
	runtime := &apiRuntimeSpy{}

	tests := map[string]func(*server.Limits){
		"connections":       func(limits *server.Limits) { limits.MaxConnections = 0 },
		"plans":             func(limits *server.Limits) { limits.MaxPlansPerConnection = 0 },
		"sessions":          func(limits *server.Limits) { limits.MaxSessionsPerConnection = 0 },
		"executions":        func(limits *server.Limits) { limits.MaxExecutionsPerConnection = 0 },
		"debug sessions":    func(limits *server.Limits) { limits.MaxDebugSessionsPerConnection = 0 },
		"watchers":          func(limits *server.Limits) { limits.MaxWatchersPerResource = 0 },
		"breakpoints":       func(limits *server.Limits) { limits.MaxBreakpointsPerDebugSession = 0 },
		"inbound messages":  func(limits *server.Limits) { limits.MaxInboundMessageBytes = 0 },
		"outbound messages": func(limits *server.Limits) { limits.MaxOutboundMessageBytes = 0 },
	}

	for name, invalidate := range tests {
		t.Run(name, func(t *testing.T) {
			limits := server.DefaultLimits()
			invalidate(&limits)
			if _, err := server.NewServer(runtime, server.WithLimits(limits)); err == nil {
				t.Fatal("NewServer accepted a non-positive limit")
			}
		})
	}

	limits := server.DefaultLimits()
	limits.MaxConnections = 3
	if _, err := server.NewServer(runtime, server.WithLimits(limits)); err != nil {
		t.Fatalf("NewServer rejected a complete positive override: %v", err)
	}
}
