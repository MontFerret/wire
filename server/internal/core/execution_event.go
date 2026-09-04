package core

import wireruntime "github.com/MontFerret/wire/pkg/runtime"

type ExecutionSubscription struct {
	Current wireruntime.Event
	Events  <-chan wireruntime.Event
	Errors  <-chan error
	Cancel  func()
}

func cloneExecutionEvent(event wireruntime.Event) wireruntime.Event {
	event.Snapshot = cloneExecutionSnapshot(event.Snapshot)

	return event
}

func sequenceExecutionEvent(event wireruntime.Event, sequence uint64) wireruntime.Event {
	event.Sequence = sequence

	return event
}
