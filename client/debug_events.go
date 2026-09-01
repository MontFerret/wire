package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

type (
	// DebugState describes the lifecycle state in a DebugSessionSnapshot.
	DebugState uint8

	// DebugEventKind identifies an ordered debug state transition. Started and
	// continued events are distinct even though both carry a running snapshot.
	DebugEventKind uint8

	// DebugSessionSnapshot is the state published for one remote debug event.
	DebugSessionSnapshot struct {
		State            DebugState
		StopReason       debugger.Reason
		Location         *source.Range
		HitBreakpointIDs []debugger.BreakpointID
		Output           *Output
		Failure          *Failure
	}

	// DebugEvent carries an ordered debug-session snapshot.
	DebugEvent struct {
		Sequence uint64
		Kind     DebugEventKind
		Snapshot DebugSessionSnapshot
	}

	// DebugEvents receives published debug snapshots until the terminal event or
	// stream cancellation.
	DebugEvents struct {
		stream wirev1.DebugService_WatchDebugClient
		cancel context.CancelFunc
	}
)

// Debug session lifecycle states.
const (
	DebugCreated DebugState = iota + 1
	DebugRunning
	DebugStopped
	DebugCompleted
	DebugFailed
	DebugTerminated
)

// Debug event kinds. Every session has one ordered terminal event.
const (
	DebugEventStarted DebugEventKind = iota + 1
	DebugEventContinued
	DebugEventStopped
	DebugEventCompleted
	DebugEventFailed
	DebugEventTerminated
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
func (events *DebugEvents) Recv() (DebugEvent, error) {
	if events == nil || events.stream == nil {
		return DebugEvent{}, errors.New("debug event receiver is nil")
	}

	value, err := events.stream.Recv()
	if err != nil {
		events.cancel()

		return DebugEvent{}, decodeError(err)
	}

	if value.GetPayload() == nil {
		events.cancel()

		return DebugEvent{}, fmt.Errorf("Wire server returned an empty debug event")
	}

	event, err := convertDebugEvent(value)
	if err != nil {
		events.cancel()

		return DebugEvent{}, err
	}
	if event.Snapshot.State.Terminal() {
		events.cancel()
	}

	return event, nil
}

// Terminal reports whether the debug session has reached a final state.
func (state DebugState) Terminal() bool {
	switch state {
	case DebugCompleted, DebugFailed, DebugTerminated:
		return true
	default:
		return false
	}
}
