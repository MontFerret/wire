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

func (d *DebugSession) Watch() (DebugSubscription, error) {
	subscription, err := d.events.subscribe()
	if err != nil {
		return DebugSubscription{}, resourceExhausted("debug watcher limit reached")
	}

	return DebugSubscription{
		Current: subscription.current,
		Events:  subscription.events,
		Errors:  subscription.errors,
		Cancel:  subscription.cancel,
	}, nil
}

func (d *DebugSession) publishLocked(kind DebugEventKind, terminal bool) {
	d.events.publish(DebugEvent{
		Session:  d.id,
		Kind:     kind,
		Snapshot: d.snapshotLocked(),
	}, terminal)
}

func (e DebugEvent) clone() DebugEvent {
	e.Snapshot = e.Snapshot.clone()

	return e
}

func (e DebugEvent) withSequence(sequence uint64) DebugEvent {
	e.Sequence = sequence

	return e
}
