package core

import (
	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/pkg/failure"
)

// ExecutionRecord combines server-private identity with a shared semantic
// snapshot. Registry and parent metadata never enters the shared model.
type ExecutionRecord struct {
	ID ExecutionID
	execution.Snapshot
}

func cloneExecutionSnapshot(snapshot execution.Snapshot) execution.Snapshot {
	result := snapshot

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
