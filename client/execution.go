package client

import (
	"context"
	"errors"

	"github.com/MontFerret/wire/internal/lifecycle"
)

type (
	// ExecuteOptions controls encoded execution output.
	ExecuteOptions struct {
		OutputContentType string
	}

	// Output preserves Ferret's encoded content-type and byte abstraction.
	Output struct {
		ContentType string
		Content     []byte
	}

	// Execution is one remote execution owned by its Plan.
	Execution struct {
		plan      *Plan
		transport *executionTransport
		id        string
		handle    *lifecycle.Handle
	}
)

func newExecution(plan *Plan, transport *executionTransport, id string) *Execution {
	return &Execution{plan: plan, transport: transport, id: id, handle: &lifecycle.Handle{}}
}

// Cancel requests execution cancellation. The ordered terminal cancellation
// snapshot remains observable through Watch.
func (e *Execution) Cancel(ctx context.Context) error {
	if err := e.checkOpen(); err != nil {
		return err
	}

	return e.transport.cancel(ctx, e.id)
}

// Watch opens an ordered event stream tied to both ctx and the Client's
// logical lifecycle. Its first event contains the current remote snapshot.
func (e *Execution) Watch(ctx context.Context) (*ExecutionEvents, error) {
	if err := e.checkOpen(); err != nil {
		return nil, err
	}

	watchCtx, cancel := e.plan.client.watchContext(ctx)
	stream, err := e.transport.watch(watchCtx, e.id)
	if err != nil {
		cancel()

		return nil, err
	}

	return &ExecutionEvents{stream: stream, cancel: cancel}, nil
}

// Wait observes execution events until the remote execution reaches a terminal
// state. A failed execution returns *Failure, while remote cancellation returns
// ErrExecutionCancelled. Caller cancellation returns the waiting context's
// error. Wait does not release the execution or retain mutable snapshot state.
func (e *Execution) Wait(ctx context.Context) (Output, error) {
	events, err := e.Watch(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Output{}, ctxErr
		}

		return Output{}, err
	}

	for {
		event, receiveErr := events.Recv()
		if receiveErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return Output{}, ctxErr
			}

			return Output{}, receiveErr
		}

		if !event.Snapshot.State.Terminal() {
			continue
		}

		output := event.Snapshot.output()
		switch event.Snapshot.State {
		case ExecutionCompleted:
			if event.Snapshot.Output == nil {
				return Output{}, errors.New("Wire server returned a completed execution without output")
			}

			return output, nil
		case ExecutionFailed:
			if event.Snapshot.Failure == nil {
				return output, errors.New("Wire server returned a failed execution without failure details")
			}

			return output, event.Snapshot.Failure
		case ExecutionCancelled:
			return output, ErrExecutionCancelled
		}
	}
}

// Close commits cancellation and remote execution cleanup. Concurrent and
// repeated calls observe one retained release result.
func (e *Execution) Close(ctx context.Context) error {
	if e == nil || e.plan == nil || e.transport == nil || e.id == "" || e.handle == nil {
		return ErrClosed
	}

	return e.handle.Close(ctx, e.release)
}

func (e *Execution) checkOpen() error {
	if e == nil || e.plan == nil || e.transport == nil || e.id == "" || e.handle == nil || !e.handle.Open() {
		return ErrClosed
	}

	return e.plan.checkOpen()
}

func (e *Execution) release(ctx context.Context) error {
	if closing, err := e.plan.ancestorCloseResult(ctx); closing {
		return err
	}

	if err := e.plan.client.checkOpen(); err != nil {
		return err
	}

	return e.transport.release(ctx, e.id)
}
