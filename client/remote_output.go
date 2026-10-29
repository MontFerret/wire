package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
)

// remoteOutput owns a single delivery and its execution reference. The callback
// owner alone settles active consumption; Close never releases borrowed bytes
// or finishes cleanup while a callback is running.
type remoteOutput struct {
	mu           sync.Mutex
	invocation   context.Context
	stream       wirev1.ExecutionService_ReadExecutionOutputClient
	streamCancel context.CancelFunc
	stopLifetime func() bool
	execution    *executionHandle
	drop         func() error
	decoder      outputstream.Decoder
	metadata     api.Metadata
	active       bool
	stopping     bool
	explicit     bool
	committed    bool
	cancel       context.CancelFunc
	done         chan struct{}
	cleanupErr   error
}

var _ api.Output = (*remoteOutput)(nil)

func (e *executionHandle) openOutput(ctx context.Context, drop func() error) (*remoteOutput, error) {
	if err := e.checkOpen(); err != nil {
		return nil, err
	}

	streamCtx, cancel := e.client.watchContext(ctx)
	stream, err := e.client.executionClient.ReadExecutionOutput(streamCtx, &wirev1.ReadExecutionOutputRequest{
		ConnectionId: e.client.connectionProto(), ExecutionId: &wirev1.ExecutionId{Value: e.id},
	})
	var decoder outputstream.Decoder

	if err == nil {
		var response *wirev1.ReadExecutionOutputResponse

		response, err = stream.Recv()
		if err == nil {
			if response.GetFrame().GetHeader() == nil {
				err = errors.New("Wire server returned output without a metadata header")
			} else {
				_, _, _, err = decoder.Accept(response.GetFrame())
			}
		}
	}

	if err == nil {
		err = ctx.Err()
	}

	if err != nil {
		cancel()

		return nil, errors.Join(ctx.Err(), decodeError(err), boundedCleanup(ctx, convenienceCleanupTimeout, e.Close))
	}

	output := &remoteOutput{invocation: ctx, stream: stream, streamCancel: cancel, execution: e, drop: drop,
		decoder: decoder, metadata: decoder.Metadata(), done: make(chan struct{})}
	// Install under the lock: an already canceled lifetime may schedule its
	// callback immediately, before AfterFunc returns the stop function.
	output.mu.Lock()
	output.stopLifetime = context.AfterFunc(streamCtx, output.lifetimeEnded)
	output.mu.Unlock()

	return output, nil
}

func (o *remoteOutput) Metadata() api.Metadata { return o.metadata }

func (o *remoteOutput) lifetimeEnded() {
	o.mu.Lock()
	if o.stopping {
		o.mu.Unlock()

		return
	}

	if o.active {
		cancel := o.cancel
		o.mu.Unlock()
		cancel()
		o.streamCancel()

		return
	}

	o.stopping = true
	o.mu.Unlock()
	o.settle()
}

func (o *remoteOutput) Close() error {
	o.mu.Lock()

	start := !o.stopping && !o.active
	if !o.committed {
		o.explicit = true
	}

	o.stopping = true
	cancel := o.cancel
	o.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	o.streamCancel()

	if start {
		o.settle()
	}

	<-o.done
	o.mu.Lock()
	err := o.cleanupErr
	o.mu.Unlock()

	return err
}

func (o *remoteOutput) Consume(ctx context.Context, consumer api.Consumer) (resultErr error) {
	if ctx == nil {
		return errors.New("context must not be nil")
	}

	if consumer == nil {
		return errors.New("output consumer must not be nil")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := o.invocation.Err(); err != nil {
		return err
	}

	o.mu.Lock()
	if o.stopping {
		o.mu.Unlock()

		return api.ErrOutputClosed
	}

	if o.active {
		o.mu.Unlock()

		return api.ErrOutputInUse
	}

	operation, cancel := outputContext(ctx, o.invocation)
	o.cancel = cancel
	o.active = true
	o.mu.Unlock()
	stop := context.AfterFunc(operation, o.streamCancel)
	defer func() {
		// This runs during callback panic unwinding too, without recovering it.
		resultErr = errors.Join(resultErr, operation.Err(), ctx.Err(), o.invocation.Err())
		o.mu.Lock()
		if o.explicit {
			resultErr = errors.Join(resultErr, api.ErrOutputClosed)
		}

		o.committed, o.stopping, o.active = true, true, false
		o.mu.Unlock()
		stop()
		cancel()
		o.settle()
		o.mu.Lock()
		resultErr = errors.Join(resultErr, o.cleanupErr)
		o.mu.Unlock()
	}()

	for {
		response, err := o.stream.Recv()
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}

		if err != nil {
			return decodeError(err)
		}

		data, chunk, terminal, err := o.decoder.Accept(response.GetFrame())
		if err != nil {
			return errors.Join(err, outputTerminalError(terminal))
		}

		if chunk {
			if err := operation.Err(); err != nil {
				return err
			}

			if err := consumer(operation, data); err != nil {
				return err
			}
		}

		if terminal != nil {
			return outputTerminalError(terminal)
		}
	}
}

func (o *remoteOutput) Collect(ctx context.Context) (*api.Content, error) {
	content := outputstream.Accumulator{Metadata: o.metadata}
	err := o.Consume(ctx, func(_ context.Context, data []byte) error {
		content.Add(data)

		return nil
	})

	return content.Content(), err
}

func (o *remoteOutput) settle() {
	o.stopLifetime()
	o.streamCancel()

	err := boundedCleanup(o.invocation, convenienceCleanupTimeout, o.execution.Close)
	if o.drop != nil {
		err = errors.Join(err, o.drop())
	}

	o.mu.Lock()
	o.cleanupErr = err
	close(o.done)
	o.mu.Unlock()
}

func outputContext(ctx, invocation context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := invocation.Deadline(); ok {
		if current, present := ctx.Deadline(); !present || deadline.Before(current) {
			return context.WithDeadline(ctx, deadline)
		}
	}

	// lifetimeEnded already cancels active consumption when the invocation or
	// connection ends. Only the consumer lifetime and earliest deadline remain.
	return context.WithCancel(ctx)
}

func outputTerminalError(value *wirev1.OutputTerminal) error {
	if value == nil {
		return nil
	}

	failure, err := convertFailure(value.GetFailure())
	result := err

	if failure != nil {
		result = failure
	}

	if value.GetCancelled() {
		result = errors.Join(result, ErrExecutionCancelled, context.Canceled)
	}

	if value.GetDeadlineExceeded() {
		result = errors.Join(result, context.DeadlineExceeded)
	}

	return result
}
