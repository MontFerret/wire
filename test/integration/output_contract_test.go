package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestOutputPresenceLengthAndPartialFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		present   bool
		length    int64
		data      string
		failure   error
		wantError bool
	}{
		{name: "absent"},
		{name: "empty", present: true},
		{name: "known", present: true, length: 3, data: "abc"},
		{name: "short", present: true, length: 4, data: "abc", wantError: true},
		{name: "absent mismatch", length: 1, wantError: true},
		{name: "partial", present: true, length: 8, data: "abc", failure: errors.New("host private detail"), wantError: true},
		{name: "joined cancellation", present: true, length: 8, data: "abc", failure: errors.Join(errors.New("host private detail"), context.DeadlineExceeded), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := api.Metadata{ContentType: "text/plain", Length: test.length, LengthKnown: true}
			hosted := &streamingOutputSpy{metadata: metadata, consume: func(ctx context.Context, consumer api.Consumer) error {
				if test.present {
					if err := consumer(ctx, []byte(test.data)); err != nil {
						return err
					}
				}

				return test.failure
			}}
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))

			output, err := h.Runtime().Run(h.Context(), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			content, err := output.Collect(h.Context())
			if (err != nil) != test.wantError || (content != nil) != test.present {
				t.Fatalf("content=%v error=%v", content, err)
			}

			if content != nil && (content.Metadata != metadata || string(content.Data) != test.data) {
				t.Fatalf("content=%v", content)
			}

			if err != nil && strings.Contains(err.Error(), "host private detail") {
				t.Fatal("private hosted error leaked")
			}

			if errors.Is(test.failure, context.DeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost deadline: %v", err)
			}

			if err := output.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOutputInterruptedTransferReturnsCollectedPrefix(t *testing.T) {
	hosted := &streamingOutputSpy{metadata: api.Metadata{Length: 1 << 20, LengthKnown: true}, consume: func(ctx context.Context, consumer api.Consumer) error {
		return consumer(ctx, make([]byte, 1<<20))
	}}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))
	h.Faults().InterruptStream(harness.ReadExecutionOutput, 2, status.Error(codes.Unavailable, "interrupted transfer"))

	output, err := h.Runtime().Run(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	content, err := output.Collect(h.Context())
	if status.Code(err) != codes.Unavailable || content == nil || len(content.Data) != 64<<10 || content.Metadata != hosted.metadata {
		t.Fatalf("prefix=%v error=%v", content, err)
	}

	if err := output.Close(); err != nil {
		t.Fatal(err)
	}

	if hosted.closes.Load() != 1 {
		t.Fatal("interrupted output was not finalized")
	}
}

func TestOutputConsumerErrorAndCleanupRemainObservable(t *testing.T) {
	consumerErr := errors.New("consumer rejected bytes")
	hosted := &streamingOutputSpy{consume: func(ctx context.Context, consumer api.Consumer) error { return consumer(ctx, []byte("data")) }, cleanup: func() error { return errors.New("private cleanup") }}
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))

	output, err := h.Runtime().Run(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	err = output.Consume(h.Context(), func(context.Context, []byte) error { return consumerErr })

	var cleanup *client.Error
	if !errors.Is(err, consumerErr) || !errors.As(err, &cleanup) {
		t.Fatalf("lost consumer or cleanup error: %v", err)
	}

	for range 2 {
		if err := output.Close(); err == nil || strings.Contains(err.Error(), "private cleanup") {
			t.Fatalf("cleanup=%v", err)
		}
	}

	if hosted.closes.Load() != 1 {
		t.Fatal("cleanup repeated")
	}
}

func TestOutputCancellationCombinesInvocationAndConsumption(t *testing.T) {
	for _, cancelInvocation := range []bool{false, true} {
		t.Run(map[bool]string{false: "consumer", true: "invocation"}[cancelInvocation], func(t *testing.T) {
			settled := make(chan struct{})
			hosted := &streamingOutputSpy{consume: func(ctx context.Context, consumer api.Consumer) error {
				defer close(settled)

				if err := consumer(ctx, []byte("prefix")); err != nil {
					return err
				}

				<-ctx.Done()

				return ctx.Err()
			}}
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))
			invocation, cancel := context.WithTimeout(h.Context(), time.Minute)
			defer cancel()
			consumption, stop := context.WithCancel(h.Context())
			defer stop()

			output, err := h.Runtime().Run(invocation, api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			err = output.Consume(consumption, func(ctx context.Context, _ []byte) error {
				deadline, ok := ctx.Deadline()

				want, _ := invocation.Deadline()
				if !ok || !deadline.Equal(want) {
					t.Error("callback lost invocation deadline")
				}

				if cancelInvocation {
					cancel()
				} else {
					stop()
				}

				<-ctx.Done()

				return ctx.Err()
			})
			if !errors.Is(err, context.Canceled) || errors.Is(err, api.ErrOutputClosed) {
				t.Fatalf("cancellation=%v", err)
			}

			select {
			case <-settled:
			case <-h.Context().Done():
				t.Fatal("hosted consumer did not settle")
			}

			if err := output.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDebuggerInterruptedContentPreservesSemanticFailure(t *testing.T) {
	h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Debugger: harness.DebuggerBehavior{
		Command: func(context.Context, string, int) (*debugger.Event, error) {
			return &debugger.Event{Reason: debugger.ReasonCompleted, Output: &api.Content{Data: make([]byte, 1<<20)}, Error: errors.New("private event error")}, errors.New("private command error")
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
	h.Faults().InterruptStream(harness.RunDebugCommand, 3, status.Error(codes.Unavailable, "interrupted transfer"))

	event, err := session.Start(h.Context())
	if event != nil || err == nil || !strings.Contains(err.Error(), "interrupted transfer") || !strings.Contains(err.Error(), "runtime operation failed") {
		t.Fatalf("event=%v error=%v", event, err)
	}
}

func TestHostedOutputPanicsPoisonDurableSession(t *testing.T) {
	for _, method := range []string{"Metadata", "Consume", "Close"} {
		t.Run(method, func(t *testing.T) {
			hosted := &streamingOutputSpy{consume: func(context.Context, api.Consumer) error { return nil }}
			switch method {
			case "Metadata":
				hosted.inspect = func() api.Metadata { panic("private metadata panic") }
			case "Consume":
				hosted.consume = func(context.Context, api.Consumer) error { panic("private consume panic") }
			case "Close":
				hosted.cleanup = func() error { panic("private close panic") }
			}

			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Session: func(harness.SessionOptions) harness.SessionBehavior {
				return harness.SessionBehavior{Output: func(context.Context, int) (api.Output, error) { return hosted, nil }}
			}}}))

			plan, err := h.Runtime().Compile(h.Context(), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			h.Own(plan)

			session, err := plan.NewSession(h.Context())
			if err != nil {
				t.Fatal(err)
			}

			h.Own(session)
			output, err := session.Run(h.Context())

			if method == "Metadata" {
				if output != nil || err == nil {
					t.Fatalf("preparation returned %v, %v", output, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}

				_, err = output.Collect(h.Context())
				if err == nil {
					t.Fatal("hosted panic was lost")
				}

				closeErr := output.Close()
				if (closeErr != nil) != (method == "Close") {
					t.Fatalf("cleanup=%v", closeErr)
				}
			}

			if strings.Contains(err.Error(), "private") || hosted.closes.Load() != 1 {
				t.Fatalf("unsafe output cleanup: %v", err)
			}

			if output, err := session.Run(h.Context()); output != nil || err == nil {
				t.Fatalf("poisoned session reused: %v %v", output, err)
			}
		})
	}
}

func TestSessionCanRunAgainAfterOutputSettlement(t *testing.T) {
	h := harness.New(t)

	plan, err := h.Runtime().Compile(h.Context(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	session, err := plan.NewSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(session)
	for range 3 {
		output, err := session.Run(h.Context())
		if err != nil {
			t.Fatal(err)
		}

		if _, err := output.Collect(h.Context()); err != nil {
			t.Fatal(err)
		}

		if err := output.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUnreadOutputFinalizesAfterLifetimeEnds(t *testing.T) {
	for _, mode := range []string{"invocation", "disconnect", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			cleaned := make(chan struct{})
			hosted := &streamingOutputSpy{consume: func(ctx context.Context, consumer api.Consumer) error {
				chunk := make([]byte, 64<<10)
				for {
					if err := consumer(ctx, chunk); err != nil {
						return err
					}
				}
			}, cleanup: func() error { close(cleaned); return nil }}
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) { return hosted, nil }}))
			ctx, cancel := context.WithCancel(h.Context())
			defer cancel()

			output, err := h.Runtime().Run(ctx, api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			switch mode {
			case "invocation":
				cancel()
			case "disconnect":
				if err := h.CloseTransport(); err != nil {
					t.Fatal(err)
				}
			case "shutdown":
				if err := h.Shutdown(); err != nil {
					t.Fatal(err)
				}
			}

			select {
			case <-cleaned:
			case <-h.Context().Done():
				t.Fatal("unread output retained hosted resources")
			}

			if _, err := output.Collect(h.Context()); err == nil {
				t.Fatal("expired output remained consumable")
			}

			if err := output.Close(); !expectedConnectionLoss(err, "runtime", mode == "shutdown") {
				t.Fatal(err)
			}

			if hosted.closes.Load() != 1 {
				t.Fatal("output cleanup repeated")
			}
		})
	}
}

func TestHostedOutputPreparationRejectsInvalidHandles(t *testing.T) {
	for _, mode := range []string{"nil", "typed nil", "invalid metadata", "output with preparation error"} {
		t.Run(mode, func(t *testing.T) {
			hosted := &streamingOutputSpy{metadata: api.Metadata{Length: -1, LengthKnown: true}, consume: func(context.Context, api.Consumer) error { t.Error("invalid output was consumed"); return nil }}
			h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) {
				switch mode {
				case "nil":
					return nil, nil
				case "typed nil":
					return (*streamingOutputSpy)(nil), nil
				case "output with preparation error":
					return hosted, errors.New("private preparation failure")
				default:
					return hosted, nil
				}
			}}))

			output, err := h.Runtime().Run(h.Context(), api.Source{})
			if output != nil || err == nil || strings.Contains(err.Error(), "private") {
				t.Fatalf("Run=%v error=%v", output, err)
			}

			if mode != "nil" && mode != "typed nil" && hosted.closes.Load() != 1 {
				t.Fatal("unpublished output was not closed once")
			}
		})
	}
}
