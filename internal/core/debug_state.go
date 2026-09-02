package core

type DebugState uint8

func (s DebugState) terminal() bool {
	return s == DebugCompleted || s == DebugFailed || s == DebugTerminated
}
