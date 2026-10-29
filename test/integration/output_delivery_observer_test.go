package integration_test

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

type (
	// outputDeliveryObserver is installed only in diagnostic tests, never in
	// timed or allocation-profile benchmark runs. It retains counts, not frames.
	outputDeliveryObserver struct {
		producing atomic.Int64
		produced  atomic.Int64
		sending   atomic.Int64
		sent      atomic.Int64
		headers   atomic.Int64
		terminals atomic.Int64
		done      chan struct{}
	}

	outputDeliveryStream struct {
		grpc.ServerStream
		observer *outputDeliveryObserver
	}
)

func (o *outputDeliveryObserver) intercept(service any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if info.FullMethod != wirev1.ExecutionService_ReadExecutionOutput_FullMethodName {
		return handler(service, stream)
	}

	defer close(o.done)

	return handler(service, &outputDeliveryStream{ServerStream: stream, observer: o})
}

func (o *outputDeliveryObserver) output(fixture *outputDeliveryFixture) *streamingOutputSpy {
	return &streamingOutputSpy{metadata: fixture.Metadata(), consume: func(ctx context.Context, consumer api.Consumer) error {
		return fixture.Consume(ctx, func(ctx context.Context, data []byte) error {
			o.producing.Add(1)

			err := consumer(ctx, data)
			if err == nil {
				o.produced.Add(1)
			}

			return err
		})
	}}
}

func (s *outputDeliveryStream) SendMsg(message any) error {
	response := message.(*wirev1.ReadExecutionOutputResponse)

	_, chunk := response.GetFrame().GetFrame().(*wirev1.OutputFrame_Chunk)
	if chunk {
		s.observer.sending.Add(1)
	}

	if err := s.ServerStream.SendMsg(message); err != nil {
		return err
	}

	switch response.GetFrame().GetFrame().(type) {
	case *wirev1.OutputFrame_Header:
		s.observer.headers.Add(1)
	case *wirev1.OutputFrame_Chunk:
		s.observer.sent.Add(1)
	case *wirev1.OutputFrame_Terminal:
		s.observer.terminals.Add(1)
	}

	return nil
}
