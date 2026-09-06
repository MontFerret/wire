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

func (l fixtureLimits) resources() ResourceLimits {
	return ResourceLimits{
		Plans: l.MaxPlansPerConnection, Sessions: l.MaxSessionsPerConnection,
		Executions: l.MaxExecutionsPerConnection, DebugSessions: l.MaxDebugSessionsPerConnection,
		Watchers: l.MaxWatchersPerResource, Breakpoints: l.MaxBreakpointsPerDebugSession,
	}
}
