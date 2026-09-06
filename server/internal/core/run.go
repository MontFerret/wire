package core

import (
	"context"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

// Run creates an asynchronous execution of exactly one hosted Runtime.Run call.
func Run(ctx context.Context, runtime api.Runtime, store *ResourceStore, source api.Source, options ...api.SessionOption) (*Execution, error) {
	if err := store.operationError(ctx); err != nil {
		return nil, err
	}

	if source.Content == "" {
		return nil, invalidRequest("source content is required")
	}

	if source.Name == "" {
		source.Name = "anonymous"
	}

	if err := store.beginCreation(executionResource, nil); err != nil {
		return nil, err
	}

	committed := false
	defer func() { store.finishCreation(executionResource, nil, committed) }()

	created := newExecution(store, nil, nil, func(runCtx context.Context) (api.Output, error) {
		output, err := panicboundary.Call(func() (api.Output, error) {
			return runtime.Run(runCtx, source, options...)
		})

		return output, runtimePanicError("run hosted runtime", err)
	}, nil)
	if err := store.registerExecution(ctx, created); err != nil {
		created.cancel(context.Canceled)

		return nil, err
	}

	committed = true
	go created.run()

	return created, nil
}
