package client

import (
	"errors"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

func remoteDebuggerEvent(event wiredebugger.Event) (*debugger.Event, bool, error) {
	snapshot := event.Snapshot
	switch snapshot.State {
	case wiredebugger.StateCreated, wiredebugger.StateRunning:
		return nil, false, nil
	case wiredebugger.StateFailed:
		if snapshot.Failure == nil {
			return nil, false, errors.New("Wire server returned a failed debug session without failure details")
		}

		return nil, false, snapshot.Failure
	case wiredebugger.StateStopped:
		result := &debugger.Event{
			Reason:           snapshot.StopReason,
			HitBreakpointIDs: append([]debugger.BreakpointID(nil), snapshot.HitBreakpointIDs...),
			Depth:            snapshot.Depth,
		}
		if snapshot.Failure != nil {
			result.Error = snapshot.Failure
		}

		if snapshot.Location != nil {
			result.Location = *snapshot.Location
		}

		return result, true, nil
	case wiredebugger.StateCompleted:
		result := &debugger.Event{Reason: debugger.ReasonCompleted}
		if snapshot.Output != nil {
			result.Output = &api.Output{
				ContentType: snapshot.Output.ContentType,
				Content:     append([]byte(nil), snapshot.Output.Content...),
			}
		}

		return result, true, nil
	case wiredebugger.StateTerminated:
		return &debugger.Event{Reason: debugger.ReasonTerminated}, true, nil
	default:
		return nil, false, errors.New("Wire server returned an invalid debug session state")
	}
}
