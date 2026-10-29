package grpcserver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/diagnostics"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

type (
	executionOutputRuntime struct {
		api.Runtime
		run func(context.Context) (api.Output, error)
	}

	executionOutputSpy struct {
		api.Output
		metadata api.Metadata
		produce  func(context.Context, api.Consumer) error
		cleanup  func() error
		closes   atomic.Int32
	}

	executionOutputStream struct {
		grpc.ServerStream
		ctx  context.Context
		send func(*wirev1.ReadExecutionOutputResponse) error
	}
)

func (r *executionOutputRuntime) Run(ctx context.Context, _ api.Source, _ ...api.SessionOption) (api.Output, error) {
	return r.run(ctx)
}

func (o *executionOutputSpy) Metadata() api.Metadata { return o.metadata }

func (o *executionOutputSpy) Consume(ctx context.Context, consumer api.Consumer) error {
	return o.produce(ctx, consumer)
}

func (o *executionOutputSpy) Close() error {
	o.closes.Add(1)

	if o.cleanup != nil {
		return o.cleanup()
	}

	return nil
}

func (s *executionOutputStream) Context() context.Context { return s.ctx }

func (s *executionOutputStream) Send(value *wirev1.ReadExecutionOutputResponse) error {
	return s.send(value)
}

func newOutputExecution(t *testing.T, run func(context.Context) (api.Output, error)) (*ExecutionService, *core.Execution, *wirev1.ReadExecutionOutputRequest) {
	t.Helper()
	registry := core.NewConnectionRegistry(1, core.ResourceLimits{Executions: 1, Watchers: 2})

	connection, err := registry.Open()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := registry.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	execution, err := core.Run(t.Context(), &executionOutputRuntime{run: run}, connection.Resources(), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	return &ExecutionService{connections: registry, maxOutbound: 1024}, execution, &wirev1.ReadExecutionOutputRequest{
		ConnectionId: &wirev1.ConnectionId{Value: string(connection.ID())}, ExecutionId: &wirev1.ExecutionId{Value: string(execution.ID())},
	}
}

func TestExecutionOutputBlockedSendRetainsCleanupOwner(t *testing.T) {
	for _, source := range []string{"request", "execution", "disconnect", "shutdown"} {
		t.Run(source, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			sending, unblockSend := make(chan struct{}), make(chan struct{})
			cleanupStarted, unblockCleanup := make(chan struct{}), make(chan struct{})
			releaseSend := sync.OnceFunc(func() { close(unblockSend) })
			releaseCleanup := sync.OnceFunc(func() { close(unblockCleanup) })
			defer releaseSend()
			defer releaseCleanup()
			var produced atomic.Int32
			hosted := &executionOutputSpy{produce: func(ctx context.Context, consumer api.Consumer) error {
				for range 1000000 {
					produced.Add(1)

					if err := consumer(ctx, make([]byte, 512)); err != nil {
						return err
					}
				}

				return nil
			}, cleanup: func() error {
				close(cleanupStarted)
				<-unblockCleanup

				return nil
			}}
			service, execution, request := newOutputExecution(t, func(context.Context) (api.Output, error) { return hosted, nil })
			requestCtx, stopRequest := context.WithCancel(ctx)
			defer stopRequest()
			stream := &executionOutputStream{ctx: requestCtx, send: func(value *wirev1.ReadExecutionOutputResponse) error {
				if _, chunk := value.GetFrame().GetFrame().(*wirev1.OutputFrame_Chunk); chunk {
					close(sending)
					<-unblockSend
				}

				return nil
			}}
			result := make(chan error, 1)
			go func() { result <- service.ReadExecutionOutput(request, stream) }()
			select {
			case <-sending:
			case <-ctx.Done():
				t.Fatal("sender never reached its blocked chunk")
			}

			teardown := make(chan error, 1)
			switch source {
			case "request":
				stopRequest()
			case "execution":
				execution.Cancel()
			case "disconnect":
				go func() {
					teardown <- service.connections.CloseConnection(ctx, core.ConnectionID(request.GetConnectionId().GetValue()))
				}()
			case "shutdown":
				go func() { teardown <- service.connections.Close(ctx) }()
			}

			select {
			case err := <-result:
				if status.Code(err) != codes.Canceled {
					t.Fatalf("cancellation = %v", err)
				}
			case <-ctx.Done():
				t.Fatal("handler waited for a blocked Send or hosted cleanup")
			}

			select {
			case <-cleanupStarted:
			case <-ctx.Done():
				t.Fatal("blocked sender retained hosted producer")
			}

			if produced.Load() > 2 || hosted.closes.Load() != 1 {
				t.Fatalf("produced=%d closes=%d", produced.Load(), hosted.closes.Load())
			}

			// Capacity and the durable cleanup result remain owned until Close ends.
			waitCtx, stopWait := context.WithCancel(ctx)
			stopWait()

			if err := execution.Release(waitCtx); !errors.Is(err, context.Canceled) {
				t.Fatalf("release certified unfinished cleanup: %v", err)
			}

			if source == "disconnect" || source == "shutdown" {
				select {
				case err := <-teardown:
					t.Fatalf("teardown settled before hosted cleanup: %v", err)
				default:
				}
			}

			releaseCleanup()
			releaseSend()

			if err := execution.Release(ctx); err != nil {
				t.Fatal(err)
			}

			if source == "disconnect" || source == "shutdown" {
				select {
				case err := <-teardown:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("teardown never settled")
				}
			}
		})
	}
}

func TestExecutionOutputPreparationCancellationKeepsOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	hosted := &executionOutputSpy{produce: func(context.Context, api.Consumer) error {
		t.Error("abandoned preparation was consumed")

		return nil
	}}
	service, execution, request := newOutputExecution(t, func(context.Context) (api.Output, error) {
		close(started)
		<-unblock

		return hosted, nil
	})
	requestCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	stream := &executionOutputStream{ctx: requestCtx, send: func(*wirev1.ReadExecutionOutputResponse) error {
		t.Error("abandoned preparation sent a frame")

		return nil
	}}
	result := make(chan error, 1)
	go func() { result <- service.ReadExecutionOutput(request, stream) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("preparation never began")
	}

	stop(errors.New("request canceled"))
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("preparation cancellation=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("handler waited for preparation")
	}

	release()

	if err := execution.Release(ctx); err != nil || hosted.closes.Load() != 1 {
		t.Fatalf("late preparation cleanup=%v closes=%d", err, hosted.closes.Load())
	}
}

func TestExecutionOutputPreservesRequestContext(t *testing.T) {
	type contextKey struct{}
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), contextKey{}, "request"), 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	hosted := &executionOutputSpy{produce: func(received context.Context, consumer api.Consumer) error {
		actual, present := received.Deadline()
		if !present || actual != deadline || received.Value(contextKey{}) != "request" {
			t.Error("hosted consumption lost request values or deadline")
		}

		return consumer(received, []byte("result"))
	}}
	service, _, request := newOutputExecution(t, func(context.Context) (api.Output, error) { return hosted, nil })

	stream := &executionOutputStream{ctx: ctx, send: func(*wirev1.ReadExecutionOutputResponse) error { return nil }}
	if err := service.ReadExecutionOutput(request, stream); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionOutputSendFailureStages(t *testing.T) {
	for _, stage := range []string{"header", "chunk", "terminal"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			hosted := &executionOutputSpy{produce: func(ctx context.Context, consumer api.Consumer) error {
				return consumer(ctx, []byte("partial"))
			}}
			service, execution, request := newOutputExecution(t, func(context.Context) (api.Output, error) { return hosted, nil })
			failure := status.Error(codes.Unavailable, "delivery failed")
			stream := &executionOutputStream{ctx: ctx, send: func(value *wirev1.ReadExecutionOutputResponse) error {
				frame := value.GetFrame()
				if stage == "header" && frame.GetHeader() != nil || stage == "chunk" && len(frame.GetChunk()) != 0 || stage == "terminal" && frame.GetTerminal() != nil {
					return failure
				}

				return nil
			}}

			if err := service.ReadExecutionOutput(request, stream); status.Code(err) != codes.Unavailable {
				t.Fatalf("lost send failure: %v", err)
			}

			if err := execution.Release(ctx); err != nil || hosted.closes.Load() != 1 {
				t.Fatalf("cleanup error=%v closes=%d", err, hosted.closes.Load())
			}
		})
	}
}

func TestExecutionOutputWaitsForTerminalSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	terminal, unblock := make(chan struct{}), make(chan struct{})
	defer close(unblock)
	hosted := &executionOutputSpy{produce: func(context.Context, api.Consumer) error { return nil }}
	service, execution, request := newOutputExecution(t, func(context.Context) (api.Output, error) { return hosted, nil })
	stream := &executionOutputStream{ctx: ctx, send: func(value *wirev1.ReadExecutionOutputResponse) error {
		if value.GetFrame().GetTerminal() != nil {
			close(terminal)
			<-unblock
		}

		return nil
	}}
	result := make(chan error, 1)
	go func() { result <- service.ReadExecutionOutput(request, stream) }()
	select {
	case <-terminal:
	case <-ctx.Done():
		t.Fatal("terminal never reached Send")
	}

	select {
	case err := <-result:
		t.Fatalf("handler completed before terminal delivery: %v", err)
	default:
	}

	execution.Cancel()
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("terminal Send prevented cancellation")
	}
}

func TestExecutionOutputOutboundLimitFailures(t *testing.T) {
	for _, stage := range []string{"header", "terminal", "chunk budget"} {
		t.Run(stage, func(t *testing.T) {
			var consumed atomic.Int32
			hosted := &executionOutputSpy{metadata: api.Metadata{}, produce: func(context.Context, api.Consumer) error {
				consumed.Add(1)

				return diagnostics.Diagnostics{{Message: strings.Repeat("diagnostic", 512)}}
			}}

			if stage == "header" {
				hosted.metadata.ContentType = strings.Repeat("x", 2048)
			}

			service, execution, request := newOutputExecution(t, func(context.Context) (api.Output, error) { return hosted, nil })

			if stage == "chunk budget" {
				service.maxOutbound = 1
			}

			stream := &executionOutputStream{ctx: testDeliveryContext(t), send: func(*wirev1.ReadExecutionOutputResponse) error { return nil }}

			err := service.ReadExecutionOutput(request, stream)
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("%s limit=%v", stage, err)
			}

			if err := execution.Release(testDeliveryContext(t)); err != nil {
				t.Fatal(err)
			}

			if hosted.closes.Load() != 1 || stage != "terminal" && consumed.Load() != 0 {
				t.Fatalf("closes=%d consumes=%d", hosted.closes.Load(), consumed.Load())
			}
		})
	}
}

func TestExecutionOutputHostedStatusesRemainPrivate(t *testing.T) {
	for _, stage := range []string{"preparation", "consumption"} {
		t.Run(stage, func(t *testing.T) {
			secret := status.Error(codes.ResourceExhausted, "private hosted status")
			hosted := &executionOutputSpy{produce: func(context.Context, api.Consumer) error { return secret }}
			service, execution, request := newOutputExecution(t, func(context.Context) (api.Output, error) {
				if stage == "preparation" {
					return nil, secret
				}

				return hosted, nil
			})
			var terminal *wirev1.OutputTerminal
			stream := &executionOutputStream{ctx: testDeliveryContext(t), send: func(response *wirev1.ReadExecutionOutputResponse) error {
				if value := response.GetFrame().GetTerminal(); value != nil {
					terminal = value
				}

				return nil
			}}

			err := service.ReadExecutionOutput(request, stream)
			if stage == "preparation" && (status.Code(err) != codes.Internal || strings.Contains(err.Error(), "private")) {
				t.Fatalf("hosted status escaped: %v", err)
			}

			if stage == "consumption" && (err != nil || terminal.GetFailure() == nil || strings.Contains(terminal.GetFailure().GetMessage(), "private")) {
				t.Fatalf("hosted terminal=%v error=%v", terminal, err)
			}

			if err := execution.Release(testDeliveryContext(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func testDeliveryContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	return ctx
}
