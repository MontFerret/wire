package client

import (
	"context"
	"errors"
	"fmt"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/debugger"
)

// debugEvents receives published debug snapshots until the terminal event or
// stream cancellation.
type debugEvents struct {
	stream wirev1.DebugService_WatchDebugClient
	cancel context.CancelFunc
}

// Recv blocks for the next ordered debug event. It releases the local stream
// when a terminal event or error is observed.
func (events *debugEvents) Recv() (debugger.Event, error) {
	if events == nil || events.stream == nil {
		return debugger.Event{}, errors.New("debug event receiver is nil")
	}

	value, err := events.stream.Recv()
	if err != nil {
		events.cancel()

		return debugger.Event{}, decodeError(err)
	}

	if value.GetSession() == nil || value.GetKind() == wirev1.DebugEventKind_DEBUG_EVENT_KIND_UNSPECIFIED {
		events.cancel()

		return debugger.Event{}, fmt.Errorf("Wire server returned an empty debug event")
	}

	event, err := convertDebugEvent(value)
	if err != nil {
		events.cancel()

		return debugger.Event{}, err
	}

	if event.Snapshot.State.Terminal() {
		events.cancel()
	}

	return event, nil
}
