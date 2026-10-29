package core

import (
	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/pkg/failure"
)

func cloneExecutionSnapshot(snapshot execution.Snapshot) execution.Snapshot {
	result := snapshot
	if snapshot.Metadata != nil {
		metadata := *snapshot.Metadata
		result.Metadata = &metadata
	}

	if snapshot.Failure != nil {
		result.Failure = &failure.Failure{Category: snapshot.Failure.Category, Message: snapshot.Failure.Message, Diagnostics: cloneDiagnostics(snapshot.Failure.Diagnostics)}
	}

	return result
}

func cloneOutput(content *api.Content) *api.Content {
	if content == nil {
		return nil
	}

	return &api.Content{Metadata: content.Metadata, Data: append(content.Data[:0:0], content.Data...)}
}
