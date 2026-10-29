package grpcserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// executionDelivery owns one sender while the execution owns production. Its
// unbuffered handoff retains at most one sending message and one producer message.
// Cancellation never closes messages: hosted callbacks may outlive the handler.
type executionDelivery struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	lifetime context.Context
	limit    int
	messages chan *wirev1.ReadExecutionOutputResponse
	done     chan struct{}
	err      error // Published by closing done; only the sender writes it.
}

func newExecutionDelivery(ctx, lifetime context.Context, limit int) *executionDelivery {
	operation, cancel := context.WithCancelCause(ctx)

	return &executionDelivery{
		ctx: operation, cancel: cancel, lifetime: lifetime, limit: limit,
		messages: make(chan *wirev1.ReadExecutionOutputResponse), done: make(chan struct{}),
	}
}

func (d *executionDelivery) run(send func(*wirev1.ReadExecutionOutputResponse) error) {
	for {
		select {
		case <-d.ctx.Done():
			return
		case message := <-d.messages:
			if err := send(message); err != nil {
				d.err = err
				close(d.done)
				d.cancel(err)

				return
			}

			if message.GetFrame().GetTerminal() != nil {
				close(d.done)

				return
			}
		}
	}
}

func (d *executionDelivery) send(frame *wirev1.OutputFrame) error {
	message := &wirev1.ReadExecutionOutputResponse{Frame: frame}
	if proto.Size(message) > d.limit {
		return status.Error(codes.ResourceExhausted, "outbound message limit exceeded")
	}

	select {
	case d.messages <- message:
		return nil
	case <-d.ctx.Done():
		return d.abort(errors.Join(d.ctx.Err(), context.Cause(d.ctx)))
	case <-d.lifetime.Done():
		return d.abort(errors.Join(d.lifetime.Err(), context.Cause(d.lifetime)))
	}
}

// wait confirms the terminal Send completed. Cancellation returns independently
// of a stalled Send; returning the RPC handler then unblocks gRPC's sender.
func (d *executionDelivery) wait() error {
	select {
	case <-d.done:
		return d.err
	case <-d.ctx.Done():
		return d.abort(errors.Join(d.ctx.Err(), context.Cause(d.ctx)))
	case <-d.lifetime.Done():
		return d.abort(errors.Join(d.lifetime.Err(), context.Cause(d.lifetime)))
	}
}

// A send failure is published before it cancels the producer. Preserve that
// transport outcome instead of remapping its induced cancellation or sanitizing
// it as though it came from hosted runtime code.
func (d *executionDelivery) abort(err error) error {
	select {
	case <-d.done:
		if d.err != nil {
			return d.err
		}
	default:
	}

	return rpcError(err)
}
