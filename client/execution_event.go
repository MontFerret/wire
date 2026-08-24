package client

import (
	"context"
	"errors"
)

type (
	// ExecutionEvent carries an ordered execution snapshot.
	ExecutionEvent struct {
		Sequence uint64
		Snapshot ExecutionSnapshot
	}

	// ExecutionEvents receives the current execution snapshot followed by ordered
	// state changes until the terminal event or stream cancellation.
	ExecutionEvents struct {
		stream *executionEventStream
		cancel context.CancelFunc
	}
)

// Recv blocks for the next ordered execution event. It releases the local
// stream when a terminal event or error is observed.
func (events *ExecutionEvents) Recv() (ExecutionEvent, error) {
	if events == nil || events.stream == nil {
		return ExecutionEvent{}, errors.New("execution event receiver is nil")
	}

	event, err := events.stream.recv()
	if err != nil {
		events.cancel()

		return ExecutionEvent{}, err
	}

	if event.Snapshot.State.Terminal() {
		events.cancel()
	}

	return event, nil
}
