package integration_test

import (
	"context"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/test/integration/harness"
)

func BenchmarkExecutionOutput(b *testing.B) {
	for _, test := range []struct {
		name string
		size int
	}{{"small", 64}, {"large", 8 << 20}} {
		b.Run(test.name, func(b *testing.B) {
			payload := make([]byte, test.size)
			h := harness.New(b, harness.WithBehavior(harness.RuntimeBehavior{Output: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) {
				return &streamingOutputSpy{metadata: api.Metadata{Length: int64(len(payload)), LengthKnown: true}, consume: func(ctx context.Context, consumer api.Consumer) error { return consumer(ctx, payload) }}, nil
			}}))
			b.SetBytes(int64(test.size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				output, err := h.Runtime().Run(h.Context(), api.Source{})
				if err != nil {
					b.Fatal(err)
				}

				if err := output.Consume(h.Context(), func(context.Context, []byte) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
