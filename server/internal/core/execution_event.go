package core

import "github.com/MontFerret/wire/pkg/execution"

// ExecutionSubscription pairs a current snapshot with subsequent ordered events.
// Cancel releases the watcher slot, including after the event channels close.
type ExecutionSubscription struct {
	Current execution.Event
	Events  <-chan execution.Event
	Errors  <-chan error
	Cancel  func()
}

func cloneExecutionEvent(event execution.Event) execution.Event {
	event.Snapshot = cloneExecutionSnapshot(event.Snapshot)

	return event
}

func sequenceExecutionEvent(event execution.Event, sequence uint64) execution.Event {
	event.Sequence = sequence

	return event
}
