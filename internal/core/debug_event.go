package core

type (
	DebugEventKind uint8

	DebugEvent struct {
		Session  DebugSessionID
		Sequence uint64
		Kind     DebugEventKind
		Snapshot DebugSnapshot
	}
)

func (e DebugEvent) clone() DebugEvent {
	e.Snapshot = e.Snapshot.clone()

	return e
}
