package core

type fixtureLimits struct {
	MaxConnections                int
	MaxPlansPerConnection         int
	MaxSessionsPerConnection      int
	MaxExecutionsPerConnection    int
	MaxDebugSessionsPerConnection int
	MaxWatchersPerResource        int
	MaxBreakpointsPerDebugSession int
}
