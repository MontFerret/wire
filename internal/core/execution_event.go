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

func (e ExecutionEvent) clone() ExecutionEvent {
	e.Snapshot = e.Snapshot.clone()

	return e
}

func (e ExecutionEvent) withSequence(sequence uint64) ExecutionEvent {
	e.Sequence = sequence

	return e
}
