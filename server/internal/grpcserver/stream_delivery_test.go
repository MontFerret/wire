package grpcserver

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func TestBlockedOutputSendDoesNotHoldProducerCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	lifetime, stop := context.WithCancel(ctx)
	defer stop()
	sending, unblock, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(unblock)
	var produced atomic.Int64
	result := make(chan error, 1)
	go func() {
		result <- deliverStream(ctx, lifetime, 1024, func(*wirev1.OutputFrame) error {
			close(sending)
			<-unblock

			return context.Canceled
		}, func(_ context.Context, send func(*wirev1.OutputFrame) error) error {
			defer close(cleaned)
			for range 1000000 {
				produced.Add(1)

				if err := send(&wirev1.OutputFrame{Frame: &wirev1.OutputFrame_Chunk{Chunk: make([]byte, 512)}}); err != nil {
					return err
				}
			}

			return nil
		})
	}()
	select {
	case <-sending:
	case <-ctx.Done():
		t.Fatal("send never began")
	}

	stop()
	select {
	case <-cleaned:
	case <-ctx.Done():
		t.Fatal("stalled network retained producer")
	}

	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("handler did not return")
	}

	if produced.Load() > 2 {
		t.Fatalf("unbounded output buffering: %d chunks", produced.Load())
	}
}

func TestConnectRejectsUnsupportedOutputFramingBeforeHosting(t *testing.T) {
	for _, version := range []uint32{0, 2} {
		service := &RuntimeService{connections: core.NewConnectionRegistry(1, core.ResourceLimits{})}

		stream := &connectStream{ctx: t.Context(), send: func() error { return errors.New("unexpected handshake") }}
		if err := service.Connect(&wirev1.ConnectRequest{OutputFramingVersion: version}, stream); status.Code(err) != codes.Unimplemented {
			t.Fatalf("version=%d error=%v", version, err)
		}

		if stream.response != nil {
			t.Fatal("incompatible peer allocated a connection")
		}
	}
}

func TestDebuggerDeliveryRejectsOversizedSemanticEnvelope(t *testing.T) {
	var sent atomic.Int32

	err := deliverStream(t.Context(), t.Context(), 1024, func(*wirev1.RunCommandResponse) error {
		sent.Add(1)

		return nil
	}, func(_ context.Context, send func(*wirev1.RunCommandResponse) error) error {
		return send(&wirev1.RunCommandResponse{Result: &wirev1.DebugCommandResult{Error: &wirev1.Failure{Message: strings.Repeat("x", 2048)}}})
	})
	if status.Code(err) != codes.ResourceExhausted || sent.Load() != 0 {
		t.Fatalf("sent=%d error=%v", sent.Load(), err)
	}
}
