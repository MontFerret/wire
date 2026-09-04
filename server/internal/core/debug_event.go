package core

import wiredebugger "github.com/MontFerret/wire/pkg/debugger"

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
