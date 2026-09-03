package core

type (
	DebugEventKind uint8

	DebugEvent struct {
		Session  DebugSessionID
		Sequence uint64
		Kind     DebugEventKind
		Snapshot DebugSnapshot
	}

	DebugSubscription struct {
		Current DebugEvent
		Events  <-chan DebugEvent
		Errors  <-chan error
		Cancel  func()
	}
)

const (
	DebugEventStarted DebugEventKind = iota + 1
	DebugEventContinued
	DebugEventStopped
	DebugEventCompleted
	DebugEventFailed
	DebugEventTerminated
)

func (e DebugEvent) clone() DebugEvent {
	e.Snapshot = e.Snapshot.clone()

	return e
}

func (e DebugEvent) withSequence(sequence uint64) DebugEvent {
	e.Sequence = sequence

	return e
}
