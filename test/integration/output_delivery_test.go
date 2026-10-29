package integration_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestOutputDeliveryFrames(t *testing.T) {
	cases := append(outputDeliveryCases(), outputDeliveryCase{"uneven", 65549, 1009, 65})
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := &outputDeliveryFixture{payload: outputDeliveryPayload(test.size), callbackBytes: test.callbackBytes}
			observer := &outputDeliveryObserver{done: make(chan struct{})}
			hosted := observer.output(fixture)
			h := harness.New(t, harness.WithRuntime(&outputDeliveryRuntime{output: func() api.Output { return hosted }}),
				harness.WithOwnedTransport(client.WithInsecure()), harness.WithServerOptions(server.WithStreamInterceptors(observer.intercept)))

			output, err := h.Runtime().Run(h.Context(), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			var received, frames int

			consumeErr := output.Consume(h.Context(), func(_ context.Context, data []byte) error {
				if len(data) > len(fixture.payload)-received || !bytes.Equal(data, fixture.payload[received:received+len(data)]) {
					return errors.New("delivered content differs")
				}

				received += len(data)
				frames++

				return nil
			})

			closeErr := output.Close()
			if consumeErr != nil || closeErr != nil || received != test.size || frames != test.frames {
				t.Fatalf("received=%d frames=%d consume=%v close=%v", received, frames, consumeErr, closeErr)
			}

			harness.Await(t, observer.done)

			if observer.headers.Load() != 1 || observer.terminals.Load() != 1 || observer.sent.Load() != int64(test.frames) || hosted.closes.Load() != 1 {
				t.Fatal("framing or exactly-once hosted cleanup changed")
			}

			t.Logf("payload=%d callback=%d data_frames=%d headers=%d terminals=%d", test.size, test.callbackBytes, frames, observer.headers.Load(), observer.terminals.Load())
		})
	}
}

func TestOutputDeliveryFailures(t *testing.T) {
	for _, cleanupFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "terminal", true: "cleanup"}[cleanupFailure], func(t *testing.T) {
			fixture := &outputDeliveryFixture{payload: outputDeliveryPayload(1027), callbackBytes: 256}
			hosted := &streamingOutputSpy{metadata: fixture.Metadata(), consume: func(ctx context.Context, consumer api.Consumer) error {
				if err := fixture.Consume(ctx, consumer); err != nil {
					return err
				}

				if !cleanupFailure {
					return context.DeadlineExceeded
				}

				return nil
			}, cleanup: func() error {
				if cleanupFailure {
					return errors.New("private fixture cleanup failure")
				}

				return nil
			}}
			h := harness.New(t, harness.WithRuntime(&outputDeliveryRuntime{output: func() api.Output { return hosted }}), harness.WithOwnedTransport(client.WithInsecure()))

			output, err := h.Runtime().Run(h.Context(), api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			var received int

			consumeErr := output.Consume(h.Context(), func(_ context.Context, data []byte) error { received += len(data); return nil })
			if consumeErr == nil || received != len(fixture.payload) || !cleanupFailure && !errors.Is(consumeErr, context.DeadlineExceeded) {
				t.Fatalf("received=%d consume=%v", received, consumeErr)
			}

			for range 2 {
				if err := output.Close(); (err != nil) != cleanupFailure {
					t.Fatalf("close=%v", err)
				}
			}

			if hosted.closes.Load() != 1 {
				t.Fatal("hosted cleanup repeated")
			}
		})
	}
}

func TestOutputDeliverySlowConsumer(t *testing.T) {
	for _, cancelWhileStalled := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "cancel"}[cancelWhileStalled], func(t *testing.T) {
			fixture := &outputDeliveryFixture{payload: outputDeliveryPayload(8 << 20), callbackBytes: 64 << 10}
			observer := &outputDeliveryObserver{done: make(chan struct{})}
			hosted := observer.output(fixture)
			h := harness.New(t, harness.WithRuntime(&outputDeliveryRuntime{output: func() api.Output { return hosted }}),
				harness.WithOwnedTransport(client.WithInsecure()), harness.WithServerOptions(server.WithStreamInterceptors(observer.intercept)))
			ctx, cancel := context.WithCancel(h.Context())
			defer cancel()
			block := harness.NewBlock(t)
			defer block.Release()
			started := time.Now()

			output, err := h.Runtime().Run(ctx, api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			first, result := make(chan time.Duration, 1), make(chan error, 1)
			var received int

			go func() {
				consumeErr := output.Consume(ctx, func(ctx context.Context, data []byte) error {
					if received == 0 {
						first <- time.Since(started)

						if err := block.Wait(ctx); err != nil {
							return err
						}
					}

					if len(data) > len(fixture.payload)-received || !bytes.Equal(data, fixture.payload[received:received+len(data)]) {
						return errors.New("delivered content differs")
					}

					received += len(data)

					return nil
				})
				result <- errors.Join(consumeErr, output.Close())
			}()
			latency := harness.Await(t, first)
			harness.Await(t, block.Started)
			t.Logf("first_payload=%s callback_bytes=%d", latency, fixture.callbackBytes)
			t.Logf("stall_entry producer_entered=%d producer_returned=%d send_entered=%d send_returned=%d", observer.producing.Load(), observer.produced.Load(), observer.sending.Load(), observer.sent.Load())
			timer := time.NewTimer(100 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			t.Logf("stall_100ms producer_entered=%d producer_returned=%d send_entered=%d send_returned=%d", observer.producing.Load(), observer.produced.Load(), observer.sending.Load(), observer.sent.Load())

			if cancelWhileStalled {
				cancel()
			} else {
				block.Release()
			}

			err = harness.Await(t, result)
			if cancelWhileStalled && !errors.Is(err, context.Canceled) || !cancelWhileStalled && (err != nil || received != len(fixture.payload)) {
				t.Fatalf("received=%d consume/close=%v", received, err)
			}

			harness.Await(t, observer.done)

			if hosted.closes.Load() != 1 {
				t.Fatal("hosted output cleanup did not settle exactly once")
			}
		})
	}
}
