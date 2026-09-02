package core

type (
	ExecutionEventKind uint8

	ExecutionEvent struct {
		Execution ExecutionID
		Sequence  uint64
		Kind      ExecutionEventKind
		Snapshot  ExecutionSnapshot
	}

	ExecutionSubscription struct {
		Current ExecutionEvent
		Events  <-chan ExecutionEvent
		Errors  <-chan error
		Cancel  func()
	}
)

const (
	ExecutionEventStarted ExecutionEventKind = iota + 1
	ExecutionEventCompleted
	ExecutionEventFailed
	ExecutionEventCancelled
)

func (e *Execution) Watch() (ExecutionSubscription, error) {
	subscription, err := e.events.subscribe()
	if err != nil {
		return ExecutionSubscription{}, resourceExhausted("execution watcher limit reached")
	}

	return ExecutionSubscription{
		Current: subscription.current,
		Events:  subscription.events,
		Errors:  subscription.errors,
		Cancel:  subscription.cancel,
	}, nil
}

func (e *Execution) publishLocked(kind ExecutionEventKind, terminal bool) {
	e.events.publish(ExecutionEvent{
		Execution: e.id,
		Kind:      kind,
		Snapshot:  e.snapshotLocked(),
	}, terminal)

	if terminal {
		close(e.done)
	}
}

func (e ExecutionEvent) clone() ExecutionEvent {
	e.Snapshot = e.Snapshot.clone()

	return e
}

func (e ExecutionEvent) withSequence(sequence uint64) ExecutionEvent {
	e.Sequence = sequence

	return e
}
