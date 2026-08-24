package client

import (
	"context"
	"errors"
	"fmt"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
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

// Terminal reports whether the debug session has reached a final state.
func (state DebugState) Terminal() bool {
	switch state {
	case DebugCompleted, DebugFailed, DebugTerminated:
		return true
	default:
		return false
	}
}

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

	event := convertDebugEvent(value)
	if event.Snapshot.State.Terminal() {
		events.cancel()
	}

	return event, nil
}

func convertDebugSessionSnapshot(value *wirev1.DebugSession) DebugSessionSnapshot {
	return DebugSessionSnapshot{
		State:            convertDebugState(value.GetState()),
		StopReason:       convertDebugStopReason(value.GetStopReason()),
		Location:         convertLocation(value.GetLocation()),
		HitBreakpointIDs: append([]uint64(nil), value.GetHitBreakpointIds()...),
		Output:           convertOutput(value.GetOutput()),
		Failure:          convertFailure(value.GetFailure()),
	}
}

func convertDebugState(value wirev1.DebugState) DebugState {
	switch value {
	case wirev1.DebugState_DEBUG_STATE_CREATED:
		return DebugCreated
	case wirev1.DebugState_DEBUG_STATE_RUNNING:
		return DebugRunning
	case wirev1.DebugState_DEBUG_STATE_STOPPED:
		return DebugStopped
	case wirev1.DebugState_DEBUG_STATE_COMPLETED:
		return DebugCompleted
	case wirev1.DebugState_DEBUG_STATE_FAILED:
		return DebugFailed
	case wirev1.DebugState_DEBUG_STATE_TERMINATED:
		return DebugTerminated
	default:
		return 0
	}
}

func convertDebugStopReason(value wirev1.DebugStopReason) DebugStopReason {
	switch value {
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_ENTRY:
		return DebugStopEntry
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT:
		return DebugStopBreakpoint
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_STEP:
		return DebugStopStep
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_PAUSE:
		return DebugStopPause
	case wirev1.DebugStopReason_DEBUG_STOP_REASON_RUNTIME_ERROR:
		return DebugStopRuntimeError
	default:
		return DebugStopNone
	}
}

func convertDebugEvent(value *wirev1.WatchDebugResponse) DebugEvent {
	result := DebugEvent{Sequence: value.GetSequence()}

	switch payload := value.GetPayload().(type) {
	case *wirev1.WatchDebugResponse_Started:
		result.Kind = DebugEventStarted
		result.Snapshot = convertDebugSessionSnapshot(payload.Started.GetSession())
	case *wirev1.WatchDebugResponse_Continued:
		result.Kind = DebugEventContinued
		result.Snapshot = convertDebugSessionSnapshot(payload.Continued.GetSession())
	case *wirev1.WatchDebugResponse_Stopped:
		result.Kind = DebugEventStopped
		result.Snapshot = convertDebugSessionSnapshot(payload.Stopped.GetSession())
	case *wirev1.WatchDebugResponse_Completed:
		result.Kind = DebugEventCompleted
		result.Snapshot = convertDebugSessionSnapshot(payload.Completed.GetSession())
	case *wirev1.WatchDebugResponse_Failed:
		result.Kind = DebugEventFailed
		result.Snapshot = convertDebugSessionSnapshot(payload.Failed.GetSession())
	case *wirev1.WatchDebugResponse_Terminated:
		result.Kind = DebugEventTerminated
		result.Snapshot = convertDebugSessionSnapshot(payload.Terminated.GetSession())
	}

	return result
}
