package integration_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
)

// streamingOutputSpy deliberately exposes borrowed storage and independently
// observable cleanup, rather than reusing the client's output implementation.
type streamingOutputSpy struct {
	metadata api.Metadata
	inspect  func() api.Metadata
	consume  func(context.Context, api.Consumer) error
	cleanup  func() error
	closes   atomic.Int32
}

func (o *streamingOutputSpy) Metadata() api.Metadata {
	if o.inspect != nil {
		return o.inspect()
	}

	return o.metadata
}
func (o *streamingOutputSpy) Consume(ctx context.Context, consumer api.Consumer) error {
	return o.consume(ctx, consumer)
}
func (o *streamingOutputSpy) Collect(context.Context) (*api.Content, error) {
	panic("Wire must stream hosted output")
}
func (o *streamingOutputSpy) Close() error {
	o.closes.Add(1)

	if o.cleanup != nil {
		return o.cleanup()
	}

	return nil
}

func TestLargeExecutionOutputUsesBoundedMessages(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "known"}[known], func(t *testing.T) {
			payload := bytes.Repeat([]byte("abcdefgh"), 1<<20)
			metadata := api.Metadata{ContentType: "application/octet-stream", Length: int64(len(payload)), LengthKnown: known}
			hosted := &streamingOutputSpy{metadata: metadata, consume: func(ctx context.Context, consume api.Consumer) error {
				return consume(ctx, payload)
			}}
			limits := server.DefaultLimits()
			limits.MaxOutboundMessageBytes = 1024
			h := harness.New(t, harness.WithServerOptions(server.WithLimits(limits)), harness.WithBehavior(harness.RuntimeBehavior{
				Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil },
			}))

			output, err := h.Runtime().Run(h.Context(), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			if output.Metadata() != metadata {
				t.Fatal("metadata was not available before consumption")
			}

			var received int

			err = output.Consume(h.Context(), func(_ context.Context, chunk []byte) error {
				if len(chunk) > 1024 || !bytes.Equal(chunk, payload[received:received+len(chunk)]) {
					t.Fatal("chunk content or size changed")
				}

				received += len(chunk)

				return nil
			})
			if err != nil || received != len(payload) {
				t.Fatalf("received=%d error=%v", received, err)
			}

			if err := output.Close(); err != nil {
				t.Fatal(err)
			}

			if hosted.closes.Load() != 1 || output.Metadata() != metadata {
				t.Fatal("cleanup count or retained metadata changed")
			}

			if _, err := output.Collect(h.Context()); !errors.Is(err, api.ErrOutputClosed) {
				t.Fatalf("second consumption=%v", err)
			}
		})
	}
}

func TestOutputCloseWaitsForCallbackAndRejectsCompetingConsumers(t *testing.T) {
	hosted := &streamingOutputSpy{consume: func(ctx context.Context, consume api.Consumer) error { return consume(ctx, []byte("content")) }}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))

	output, err := h.Runtime().Run(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	entered, unblock := make(chan struct{}), make(chan struct{})
	callbackCanceled := make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	consumed, closed := make(chan error, 1), make(chan error, 1)
	go func() {
		consumed <- output.Consume(h.Context(), func(ctx context.Context, _ []byte) error {
			close(entered)
			select {
			case <-ctx.Done():
				close(callbackCanceled)
			case <-unblock:
				return nil
			}

			<-unblock

			return ctx.Err()
		})
	}()
	select {
	case <-entered:
	case <-h.Context().Done():
		t.Fatal("callback did not start")
	}

	if _, err := output.Collect(h.Context()); !errors.Is(err, api.ErrOutputInUse) {
		t.Fatalf("competing collection=%v", err)
	}

	go func() { closed <- output.Close() }()
	select {
	case <-callbackCanceled:
	case <-h.Context().Done():
		t.Fatal("Close did not cancel callback context")
	}

	select {
	case err := <-closed:
		release()
		t.Fatalf("Close returned during callback: %v", err)
	default:
	}

	release()
	select {
	case err := <-consumed:
		if !errors.Is(err, api.ErrOutputClosed) {
			t.Fatalf("interrupted consumption=%v", err)
		}
	case <-h.Context().Done():
		t.Fatal("consumption did not settle")
	}

	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-h.Context().Done():
		t.Fatal("Close did not settle")
	}
}

func TestOutputRejectedCallsDoNotClaimAndConsumerPanicFinalizes(t *testing.T) {
	hosted := &streamingOutputSpy{consume: func(ctx context.Context, consume api.Consumer) error { return consume(ctx, []byte("x")) }}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))

	output, err := h.Runtime().Run(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	if err := output.Consume(nil, nil); err == nil { //nolint:staticcheck // Exercise the required nil-context admission check.
		t.Fatal("nil context accepted")
	}

	if err := output.Consume(h.Context(), nil); err == nil {
		t.Fatal("nil consumer accepted")
	}

	canceled, cancel := context.WithCancel(h.Context())
	cancel()

	if _, err := output.Collect(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission=%v", err)
	}

	sentinel := errors.New("consumer panic")
	func() {
		defer func() {
			if got := recover(); got != sentinel { //nolint:errorlint // Consumer panic identity must survive unchanged.
				t.Errorf("consumer panic changed: %v", got)
			}
		}()
		_ = output.Consume(h.Context(), func(context.Context, []byte) error { panic(sentinel) })
	}()

	if err := output.Close(); err != nil {
		t.Fatal(err)
	}

	if hosted.closes.Load() != 1 {
		t.Fatalf("hosted closes=%d", hosted.closes.Load())
	}
}

func TestUnreadOutputRetainsRuntimeUntilAbandonment(t *testing.T) {
	var chunks atomic.Int64
	finished := make(chan struct{})
	hosted := &streamingOutputSpy{consume: func(ctx context.Context, consume api.Consumer) error {
		defer close(finished)
		data := make([]byte, 64<<10)
		for range 1 << 20 {
			if err := consume(ctx, data); err != nil {
				return err
			}

			chunks.Add(1)
		}

		return nil
	}}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))

	output, err := h.Runtime().Run(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := h.Runtime().Run(h.Context(), api.Source{}); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("closed root admitted work: %v", err)
	}

	if err := output.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-finished:
	case <-h.Context().Done():
		t.Fatal("unread producer retained after Close")
	}

	if chunks.Load() >= 1<<20 || hosted.closes.Load() != 1 {
		t.Fatal("abandonment drained output or lost cleanup")
	}
}

func TestLargeDebuggerContentIsReconstructedBeforePublication(t *testing.T) {
	for _, withFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "terminal error"}[withFailure], func(t *testing.T) {
			payload := bytes.Repeat([]byte("debug-result"), 700000)
			metadata := api.Metadata{ContentType: "application/octet-stream", Length: int64(len(payload)), LengthKnown: true}
			var terminalErr error

			if withFailure {
				terminalErr = errors.New("private terminal error")
				metadata.Length += 100
			}

			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{
				Command: func(context.Context, string, int) (*debugger.Event, error) {
					return &debugger.Event{Reason: debugger.ReasonCompleted, Output: &api.Content{Metadata: metadata, Data: payload}, Error: terminalErr}, terminalErr
				},
			}}}))

			plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			h.Own(plan)

			session, err := plan.NewDebugSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			h.Own(session)

			event, err := session.Start(h.Context())
			if event == nil || (err != nil) != withFailure || (event.Error != nil) != withFailure {
				t.Fatalf("event=%v error=%v", event, err)
			}

			if event.Output == nil || event.Output.Metadata != metadata || !bytes.Equal(event.Output.Data, payload) {
				t.Fatal("debug content was truncated or metadata changed")
			}

			if err := session.Close(); err != nil {
				t.Fatal(err)
			}

			payload[0] = 'X'

			if event.Output.Data[0] == 'X' {
				t.Fatal("delivered content aliases hosted storage")
			}
		})
	}
}
