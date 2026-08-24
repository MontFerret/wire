package wire_test

import (
	"testing"

	"github.com/MontFerret/ferret/v2"
	"github.com/MontFerret/wire"
)

func TestDefaultServerLimits(t *testing.T) {
	limits := wire.DefaultServerLimits()
	if limits.MaxConnections != 64 ||
		limits.MaxPlansPerConnection != 128 ||
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
	engine, err := ferret.New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	tests := map[string]func(*wire.ServerLimits){
		"connections":       func(limits *wire.ServerLimits) { limits.MaxConnections = 0 },
		"plans":             func(limits *wire.ServerLimits) { limits.MaxPlansPerConnection = 0 },
		"executions":        func(limits *wire.ServerLimits) { limits.MaxExecutionsPerConnection = 0 },
		"debug sessions":    func(limits *wire.ServerLimits) { limits.MaxDebugSessionsPerConnection = 0 },
		"watchers":          func(limits *wire.ServerLimits) { limits.MaxWatchersPerResource = 0 },
		"breakpoints":       func(limits *wire.ServerLimits) { limits.MaxBreakpointsPerDebugSession = 0 },
		"inbound messages":  func(limits *wire.ServerLimits) { limits.MaxInboundMessageBytes = 0 },
		"outbound messages": func(limits *wire.ServerLimits) { limits.MaxOutboundMessageBytes = 0 },
	}

	for name, invalidate := range tests {
		t.Run(name, func(t *testing.T) {
			limits := wire.DefaultServerLimits()
			invalidate(&limits)
			if _, err := wire.NewServer(engine, wire.WithServerLimits(limits)); err == nil {
				t.Fatal("NewServer accepted a non-positive limit")
			}
		})
	}

	limits := wire.DefaultServerLimits()
	limits.MaxConnections = 3
	if _, err := wire.NewServer(engine, wire.WithServerLimits(limits)); err != nil {
		t.Fatalf("NewServer rejected a complete positive override: %v", err)
	}
}
