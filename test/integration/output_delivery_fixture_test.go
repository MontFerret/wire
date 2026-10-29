package integration_test

import (
	"context"

	"github.com/MontFerret/api"
)

type (
	outputDeliveryCase struct {
		name          string
		size          int
		callbackBytes int
		frames        int
	}

	// outputDeliveryRuntime supports only the handshake and direct Run used by
	// this fixture. It owns no call history or per-execution resource registry.
	outputDeliveryRuntime struct {
		api.Runtime
		output func() api.Output
	}

	// outputDeliveryFixture borrows immutable source storage. Wire owns its sole
	// consumption and cleanup; there is no hosted resource or materialized result.
	outputDeliveryFixture struct {
		payload       []byte
		callbackBytes int
	}
)

func (r *outputDeliveryRuntime) Version(context.Context) (api.Version, error) {
	return api.Version("output-delivery-fixture"), nil
}

func (r *outputDeliveryRuntime) Run(ctx context.Context, _ api.Source, _ ...api.SessionOption) (api.Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return r.output(), nil
}

func (r *outputDeliveryRuntime) Close() error { return nil }

func (o *outputDeliveryFixture) Metadata() api.Metadata {
	return api.Metadata{ContentType: "application/octet-stream", Length: int64(len(o.payload)), LengthKnown: true}
}

func (o *outputDeliveryFixture) Consume(ctx context.Context, consumer api.Consumer) error {
	for offset := 0; offset < len(o.payload); offset += o.callbackBytes {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := consumer(ctx, o.payload[offset:min(offset+o.callbackBytes, len(o.payload))]); err != nil {
			return err
		}
	}

	return nil
}

func (o *outputDeliveryFixture) Collect(context.Context) (*api.Content, error) {
	panic("delivery fixture must not be materialized")
}

func (o *outputDeliveryFixture) Close() error { return nil }
