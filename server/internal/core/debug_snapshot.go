package core

import (
	"github.com/MontFerret/api"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/failure"
)

// DebugSessionRecord combines server-private identity with a shared semantic
// snapshot. Registry and parent metadata never enters the shared model.
type DebugSessionRecord struct {
	ID DebugSessionID
	wiredebugger.Snapshot
}

func cloneDebugSnapshot(snapshot wiredebugger.Snapshot) wiredebugger.Snapshot {
	result := snapshot
	result.HitBreakpointIDs = append(result.HitBreakpointIDs[:0:0], snapshot.HitBreakpointIDs...)

	if snapshot.Location != nil {
		location := *snapshot.Location
		result.Location = &location
	}

	if snapshot.Output != nil {
		result.Output = &api.Output{
			ContentType: snapshot.Output.ContentType,
			Content:     append([]byte(nil), snapshot.Output.Content...),
		}
	}

	if snapshot.Failure != nil {
		result.Failure = &failure.Failure{
			Category:    snapshot.Failure.Category,
			Message:     snapshot.Failure.Message,
			Diagnostics: cloneDiagnostics(snapshot.Failure.Diagnostics),
		}
	}

	return result
}
