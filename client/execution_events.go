package client

import (
	"context"
	"errors"
	"fmt"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/execution"
)

// executionEvents receives the current execution snapshot followed by ordered
// state changes until the terminal event or stream cancellation.
type executionEvents struct {
	stream wirev1.ExecutionService_WatchExecutionClient
	cancel context.CancelFunc
}

// Recv blocks for the next ordered execution event. It releases the local
// stream when a terminal event or error is observed.
func (events *executionEvents) Recv() (execution.Event, error) {
	if events == nil || events.stream == nil {
		return execution.Event{}, errors.New("execution event receiver is nil")
	}

	value, err := events.stream.Recv()
	if err != nil {
		events.cancel()

		return execution.Event{}, decodeError(err)
	}

	if value.GetExecution() == nil {
		events.cancel()

		return execution.Event{}, fmt.Errorf("Wire server returned an empty execution event")
	}

	event, err := convertExecutionEvent(value)
	if err != nil {
		events.cancel()

		return execution.Event{}, err
	}

	if event.Snapshot.State.Terminal() {
		events.cancel()
	}

	return event, nil
}
