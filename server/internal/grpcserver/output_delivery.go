package grpcserver

import (
	"context"
	"errors"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	wirefailure "github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/core"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

// outputWriter owns framing and byte accounting for one transfer. Its send
// function takes ownership of each message; borrowed hosted chunks are copied.
type outputWriter struct {
	send   func(*wirev1.OutputFrame) error
	size   int
	bytes  uint64
	header bool
}

func newOutputWriter(limit int, envelope func(*wirev1.OutputFrame) proto.Message, send func(*wirev1.OutputFrame) error) (*outputWriter, error) {
	size, err := outputstream.MaxChunk(limit, envelope)
	if err != nil {
		return nil, outboundLimitError("outbound message limit cannot fit an output chunk")
	}

	return &outputWriter{send: send, size: size}, nil
}

func (w *outputWriter) start(metadata api.Metadata) error {
	if _, err := outputstream.Metadata(outputstream.Descriptor(metadata)); err != nil {
		return runtimeConversionError("invalid hosted output metadata")
	}

	if err := w.send(outputstream.Header(metadata)); err != nil {
		return err
	}

	w.header = true

	return nil
}

func (w *outputWriter) consume(ctx context.Context, data []byte) error {
	return outputstream.Chunks(data, w.size, func(frame *wirev1.OutputFrame) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		chunk := frame.GetChunk()
		if uint64(len(chunk)) > math.MaxUint64-w.bytes {
			return errors.New("output byte count overflow")
		}

		frame.Frame = &wirev1.OutputFrame_Chunk{Chunk: append([]byte(nil), chunk...)}
		if err := w.send(frame); err != nil {
			return err
		}

		w.bytes += uint64(len(chunk))

		return nil
	})
}

func (w *outputWriter) finish(err error) error {
	category := wirefailure.CategoryExecution
	var boundary *panicboundary.Error

	var domain *core.DomainError
	if errors.As(err, &boundary) || errors.As(err, &domain) && domain.Kind == core.ErrorKindInternal {
		category = wirefailure.CategoryInternalRuntime
	}

	converted, conversionErr := commandError(err, category)
	if conversionErr != nil {
		return rpcError(conversionErr)
	}

	return w.send(outputstream.End(&wirev1.OutputTerminal{
		TransferredBytes: w.bytes, Failure: converted,
		Cancelled: errors.Is(err, context.Canceled), DeadlineExceeded: errors.Is(err, context.DeadlineExceeded),
	}))
}

// content sends a detached debugger payload without consuming a live handle.
func (w *outputWriter) content(ctx context.Context, content *api.Content, semanticErr error) error {
	if content == nil {
		return nil
	}

	if err := w.start(content.Metadata); err != nil {
		return err
	}

	if err := w.consume(ctx, content.Data); err != nil {
		return err
	}

	return w.finish(semanticErr)
}
