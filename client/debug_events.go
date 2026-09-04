package client

import (
	"context"
	"errors"
	"fmt"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/debugger"
)

type (
	// DebugEvents receives published debug snapshots until the terminal event or
	// stream cancellation.
	DebugEvents struct {
		stream wirev1.DebugService_WatchDebugClient
		cancel context.CancelFunc
	}
)

// Watch opens an ordered event stream tied to both ctx and the Client's
// logical lifecycle. It begins with the latest state published by the server.
func (d *DebugSession) Watch(ctx context.Context) (*DebugEvents, error) {
	if err := d.checkOpen(); err != nil {
		return nil, err
	}

	watchCtx, cancel := d.client.watchContext(ctx)
	stream, err := d.client.debugClient.WatchDebug(watchCtx, &wirev1.WatchDebugRequest{
		ConnectionId: d.client.connectionProto(), DebugSessionId: &wirev1.DebugSessionId{Value: d.id},
	})
	if err != nil {
		cancel()

		return nil, decodeError(err)
	}

	return &DebugEvents{stream: stream, cancel: cancel}, nil
}

// Recv blocks for the next ordered debug event. It releases the local stream
// when a terminal event or error is observed.
func (events *DebugEvents) Recv() (debugger.Event, error) {
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
