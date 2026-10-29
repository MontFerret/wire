// Package testoutput supplies detached, buffered output for hosted test spies.
package testoutput

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
)

// Buffered is a test implementation whose terminal outcome is configured at construction.
type Buffered struct {
	mu      sync.Mutex
	ctx     context.Context
	content *api.Content
	err     error
	used    bool
	closed  bool
}

// New returns a handle even for absent content or a terminal execution failure.
func New(ctx context.Context, content *api.Content, err error) api.Output {
	return &Buffered{ctx: ctx, content: content, err: err}
}

// Metadata preserves the configured descriptor, including unknown length.
func (o *Buffered) Metadata() api.Metadata {
	if o.content == nil {
		return api.Metadata{}
	}

	return o.content.Metadata
}

// Close records abandonment without certifying the terminal execution outcome.
func (o *Buffered) Close() error { o.mu.Lock(); o.closed = true; o.mu.Unlock(); return nil }

// Consume delivers one borrowed chunk, including present empty content.
func (o *Buffered) Consume(ctx context.Context, consumer api.Consumer) error {
	if ctx == nil {
		return errors.New("nil consumption context")
	}

	if consumer == nil {
		return errors.New("nil consumer")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := o.ctx.Err(); err != nil {
		return errors.Join(err, o.err)
	}

	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()

		return api.ErrOutputClosed
	}

	if o.used {
		o.mu.Unlock()

		return api.ErrOutputInUse
	}

	o.used = true
	o.mu.Unlock()
	defer func() { _ = o.Close() }()
	var err error

	if o.content != nil {
		err = consumer(ctx, o.content.Data)
	}

	return errors.Join(err, o.err)
}

// Collect detaches configured content through the same one-shot admission.
func (o *Buffered) Collect(ctx context.Context) (*api.Content, error) {
	var content *api.Content
	err := o.Consume(ctx, func(_ context.Context, data []byte) error {
		content = &api.Content{Metadata: o.Metadata(), Data: append(data[:0:0], data...)}

		return nil
	})

	return content, err
}

// Collect observes a Run outcome in tests that are about behavior other than
// the output-handle lifecycle. Lifecycle tests consume their handles directly.
func Collect(output api.Output, err error) (content *api.Content, resultErr error) {
	if err != nil {
		return nil, err
	}

	if output == nil {
		return nil, errors.New("nil output")
	}

	defer func() { resultErr = errors.Join(resultErr, output.Close()) }()

	return output.Collect(context.Background())
}
