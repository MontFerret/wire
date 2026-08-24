package client

import (
	"context"
	"errors"
)

type (
	// DebugState describes the lifecycle state in a DebugSessionSnapshot.
	DebugState uint8

	// DebugStopReason identifies why a running session became stopped.
	DebugStopReason uint8

	// DebugEventKind identifies an ordered debug state transition. Started and
	// continued events are distinct even though both carry a running snapshot.
	DebugEventKind uint8

	// DebugSessionSnapshot is the state published for one remote debug event.
	DebugSessionSnapshot struct {
		State            DebugState
		StopReason       DebugStopReason
		Location         *Location
		HitBreakpointIDs []uint64
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
		stream *debugEventStream
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

// Debug stop reasons reported by Ferret.
const (
	DebugStopNone DebugStopReason = iota
	DebugStopEntry
	DebugStopBreakpoint
	DebugStopStep
	DebugStopPause
	DebugStopRuntimeError
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

	watchCtx, cancel := d.plan.client.watchContext(ctx)
	stream, err := d.transport.watch(watchCtx, d.id)
	if err != nil {
		cancel()

		return nil, err
	}

	return &DebugEvents{stream: stream, cancel: cancel}, nil
}

// Recv blocks for the next ordered debug event. It releases the local stream
// when a terminal event or error is observed.
func (events *DebugEvents) Recv() (DebugEvent, error) {
	if events == nil || events.stream == nil {
		return DebugEvent{}, errors.New("debug event receiver is nil")
	}

	event, err := events.stream.recv()
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
