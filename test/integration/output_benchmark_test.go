package integration_test

import (
	"context"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/test/integration/harness"
)

func outputDeliveryCases() []outputDeliveryCase {
	return []outputDeliveryCase{
		{"64B/whole", 64, 64, 1},
		{"1MiB/whole", 1 << 20, 1 << 20, 16},
		{"8MiB/whole", 8 << 20, 8 << 20, 128},
		{"1MiB/64KiB", 1 << 20, 64 << 10, 16},
		{"1MiB/4KiB", 1 << 20, 4 << 10, 256},
		{"1MiB/256B", 1 << 20, 256, 4096},
	}
}

func outputDeliveryPayload(size int) []byte {
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = byte(index % 251)
	}

	return payload
}

// BenchmarkExecutionOutputDelivery is a new series: the historical benchmark
// used bufconn and retained/scanned an ever-growing spy and RPC call history.
func BenchmarkExecutionOutputDelivery(b *testing.B) {
	for _, test := range outputDeliveryCases() {
		b.Run(test.name, func(b *testing.B) {
			payload := outputDeliveryPayload(test.size)
			hosted := &outputDeliveryRuntime{output: func() api.Output {
				return &outputDeliveryFixture{payload: payload, callbackBytes: test.callbackBytes}
			}}
			h := harness.New(b, harness.WithRuntime(hosted), harness.WithOwnedTransport(client.WithInsecure()))
			var received int

			consume := func(_ context.Context, data []byte) error {
				received += len(data)

				return nil
			}
			b.SetBytes(int64(test.size))
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				received = 0

				output, err := h.Runtime().Run(b.Context(), api.Source{})
				if err != nil {
					b.Fatal(err)
				}

				consumeErr := output.Consume(b.Context(), consume)

				closeErr := output.Close()
				if consumeErr != nil || closeErr != nil || received != test.size {
					b.Fatalf("received=%d want=%d consume=%v close=%v", received, test.size, consumeErr, closeErr)
				}
			}
		})
	}
}
