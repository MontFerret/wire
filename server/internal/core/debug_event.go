package core

import wiredebugger "github.com/MontFerret/wire/pkg/debugger"

// DebugSubscription pairs a current snapshot with subsequent ordered events.
// Cancel releases the watcher slot, including after the event channels close.
type DebugSubscription struct {
	Current wiredebugger.Event
	Events  <-chan wiredebugger.Event
	Errors  <-chan error
	Cancel  func()
}

func cloneDebugEvent(event wiredebugger.Event) wiredebugger.Event {
	event.Snapshot = cloneDebugSnapshot(event.Snapshot)

	return event
}

func sequenceDebugEvent(event wiredebugger.Event, sequence uint64) wiredebugger.Event {
	event.Sequence = sequence

	return event
}
