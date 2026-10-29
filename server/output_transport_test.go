package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/diagnostics"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	"github.com/MontFerret/wire/server"
)

type (
	outputMethodKey     struct{}
	responseCompression struct {
		mu         sync.Mutex
		headers    int
		compressed bool
	}
	transportOutput struct {
		api.Output
		metadata api.Metadata
		failure  error
		data     []byte
		closes   atomic.Int32
		consumes atomic.Int32
	}
	transportRuntime struct {
		*contractRuntime
		output *transportOutput
		calls  atomic.Int32
	}
	transportDebugger struct {
		debugger.Session
		content *api.Content
		failure error
		closes  atomic.Int32
	}
	outputTransportFixture struct {
		env         *integrationEnv
		compression *responseCompression
		ctx         context.Context
		connection  *wirev1.ConnectionId
		plan        *wirev1.PlanId
	}
	panicSendStream struct {
		grpc.ServerStream
		stage    string
		panicked *atomic.Bool
	}
)

func (o *transportOutput) Metadata() api.Metadata { return o.metadata }

func (o *transportOutput) Consume(ctx context.Context, consume api.Consumer) error {
	o.consumes.Add(1)

	return errors.Join(consume(ctx, o.data), o.failure)
}

func (o *transportOutput) Close() error {
	o.closes.Add(1)

	return nil
}

func (r *transportRuntime) Run(context.Context, api.Source, ...api.SessionOption) (api.Output, error) {
	if r.calls.Add(1) == 1 {
		return r.output, nil
	}

	return &transportOutput{metadata: r.output.metadata, data: r.output.data, failure: r.output.failure}, nil
}

func (d *transportDebugger) Start(context.Context) (*debugger.Event, error) {
	return &debugger.Event{Reason: debugger.ReasonCompleted, Output: d.content, Error: d.failure}, nil
}

func (d *transportDebugger) Close() error {
	d.closes.Add(1)

	return nil
}

func newOutputTransportFixture(t testing.TB, output *transportOutput, hosted *transportDebugger, limit int, options ...server.Option) *outputTransportFixture {
	t.Helper()
	var constructors atomic.Int32
	parent := &contractPlan{newDebugSession: func(context.Context, apiSessionOptions) (debugger.Session, error) {
		if constructors.Add(1) == 1 {
			return hosted, nil
		}

		return &transportDebugger{content: hosted.content, failure: hosted.failure}, nil
	}}
	runtime := &transportRuntime{contractRuntime: &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) { return parent, nil }}, output: output}
	limits := server.DefaultLimits()
	limits.MaxOutboundMessageBytes = limit
	limits.MaxExecutionsPerConnection = 1
	limits.MaxDebugSessionsPerConnection = 1
	limits.MaxWatchersPerResource = 1
	options = append(options, server.WithLimits(limits))
	compression := &responseCompression{}
	env := newIntegrationEnvWithDialOptions(t, runtime, []grpc.DialOption{grpc.WithStatsHandler(compression)}, options...)
	ctx := testContext(t)

	connect, err := wirev1.NewRuntimeServiceClient(env.conn).Connect(ctx, &wirev1.ConnectRequest{OutputFramingVersion: outputstream.Version})
	if err != nil {
		t.Fatal(err)
	}

	header, err := connect.Recv()
	if err != nil {
		t.Fatal(err)
	}

	compiled, err := wirev1.NewPlanServiceClient(env.conn).CompileDebug(ctx, &wirev1.CompileDebugRequest{ConnectionId: header.GetConnectionId(), Source: &wirev1.Source{Content: "query"}})
	if err != nil {
		t.Fatal(err)
	}

	return &outputTransportFixture{env: env, compression: compression, ctx: ctx, connection: header.GetConnectionId(), plan: compiled.GetPlan().GetId()}
}

func (f *outputTransportFixture) execution(t testing.TB, options ...grpc.CallOption) (func() (*wirev1.OutputFrame, error), func()) {
	t.Helper()

	created, err := wirev1.NewRuntimeServiceClient(f.env.conn).Run(f.ctx, &wirev1.RunRequest{ConnectionId: f.connection, Source: &wirev1.Source{Content: "query"}})
	if err != nil {
		t.Fatal(err)
	}

	rpc := wirev1.NewExecutionServiceClient(f.env.conn)

	stream, err := rpc.ReadExecutionOutput(f.ctx, &wirev1.ReadExecutionOutputRequest{ConnectionId: f.connection, ExecutionId: created.GetExecution().GetId()}, options...)
	if err != nil {
		t.Fatal(err)
	}

	return func() (*wirev1.OutputFrame, error) {
			response, err := stream.Recv()

			return response.GetFrame(), err
		}, func() {
			if _, err := rpc.ReleaseExecution(f.ctx, &wirev1.ReleaseExecutionRequest{ConnectionId: f.connection, ExecutionId: created.GetExecution().GetId()}); err != nil {
				t.Error(err)
			}
		}
}

func (f *outputTransportFixture) debugger(t testing.TB, watch bool, options ...grpc.CallOption) (func() (*wirev1.OutputFrame, error), func()) {
	t.Helper()
	rpc := wirev1.NewDebugServiceClient(f.env.conn)

	created, err := rpc.CreateDebugSession(f.ctx, &wirev1.CreateDebugSessionRequest{ConnectionId: f.connection, PlanId: f.plan})
	if err != nil {
		t.Fatal(err)
	}

	id := created.GetSession().GetId()
	release := func() {
		if _, err := rpc.ReleaseDebugSession(f.ctx, &wirev1.ReleaseDebugSessionRequest{ConnectionId: f.connection, DebugSessionId: id}); err != nil {
			t.Error(err)
		}
	}

	if watch {
		if _, err := rpc.Start(f.ctx, &wirev1.StartRequest{ConnectionId: f.connection, DebugSessionId: id}); err != nil {
			t.Fatal(err)
		}

		stream, err := rpc.WatchDebug(f.ctx, &wirev1.WatchDebugRequest{ConnectionId: f.connection, DebugSessionId: id}, options...)
		if err != nil {
			t.Fatal(err)
		}

		return func() (*wirev1.OutputFrame, error) {
			for {
				response, err := stream.Recv()
				if err != nil || response.GetOutputFrame() != nil {
					return response.GetOutputFrame(), err
				}
			}
		}, release
	}

	stream, err := rpc.RunCommand(f.ctx, &wirev1.RunCommandRequest{ConnectionId: f.connection, DebugSessionId: id, Command: wirev1.DebugCommand_DEBUG_COMMAND_START}, options...)
	if err != nil {
		t.Fatal(err)
	}

	return func() (*wirev1.OutputFrame, error) {
		for {
			response, err := stream.Recv()
			if err != nil || response.GetOutputFrame() != nil {
				return response.GetOutputFrame(), err
			}
		}
	}, release
}

func collectTransportFrames(receive func() (*wirev1.OutputFrame, error)) ([]byte, error) {
	var decoder outputstream.Decoder
	var data []byte
	for {
		frame, err := receive()
		if err != nil {
			return data, err
		}

		chunk, present, terminal, err := decoder.Accept(frame)
		if err != nil {
			return data, err
		}

		if present {
			data = append(data, chunk...)
		}

		if terminal != nil {
			_, err := receive()
			if errors.Is(err, io.EOF) {
				return data, nil
			}

			if err == nil {
				err = errors.New("extra frame after terminal")
			}

			return data, err
		}
	}
}

func TestFramedOutputIdentityCompression(t *testing.T) {
	payload := make([]byte, 4096)
	generator := rand.NewPCG(17, 29)
	for i := range payload {
		payload[i] = byte(generator.Uint64())
	}

	for _, method := range []string{"execution", "command", "watch"} {
		for _, test := range []struct {
			name        string
			limit       int
			gzip, early bool
		}{
			{"gzip small", 1024, true, false}, {"gzip early headers", 1024, true, true},
			{"identity small", 1024, false, false}, {"gzip default", 4 << 20, true, false},
		} {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				hosted := &transportDebugger{content: &api.Content{Data: payload}}
				output := &transportOutput{data: payload}
				f := newOutputTransportFixture(t, output, hosted, test.limit, server.WithStreamInterceptors(func(host any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
					if test.early && (info.FullMethod == wirev1.ExecutionService_ReadExecutionOutput_FullMethodName || info.FullMethod == wirev1.DebugService_RunCommand_FullMethodName || info.FullMethod == wirev1.DebugService_WatchDebug_FullMethodName) {
						if err := stream.SendHeader(metadata.Pairs("early", "true")); err != nil {
							return err
						}
					}

					return next(host, stream)
				}))
				var options []grpc.CallOption

				if test.gzip {
					options = append(options, grpc.UseCompressor(gzip.Name))
				}

				var receive func() (*wirev1.OutputFrame, error)
				var release func()

				if method == "execution" {
					receive, release = f.execution(t, options...)
				} else {
					receive, release = f.debugger(t, method == "watch", options...)
				}

				data, err := collectTransportFrames(receive)
				release()
				f.compression.mu.Lock()
				headers, compressed := f.compression.headers, f.compression.compressed
				f.compression.mu.Unlock()

				if headers != 1 || compressed {
					t.Fatalf("framed response headers=%d compressed=%v", headers, compressed)
				}

				if err != nil || !bytes.Equal(data, payload) {
					t.Fatalf("received=%d error=%v", len(data), err)
				}

				if method == "execution" && output.closes.Load() != 1 || method != "execution" && hosted.closes.Load() != 1 {
					t.Fatal("hosted cleanup count changed")
				}
			})
		}
	}
}

func (s *panicSendStream) SendMsg(message any) error {
	var frame *wirev1.OutputFrame
	switch value := message.(type) {
	case *wirev1.ReadExecutionOutputResponse:
		frame = value.GetFrame()
	case *wirev1.RunCommandResponse:
		frame = value.GetOutputFrame()
	case *wirev1.WatchDebugResponse:
		frame = value.GetOutputFrame()
	}

	matches := s.stage == "envelope" && frame == nil || s.stage == "header" && frame.GetHeader() != nil || s.stage == "chunk" && frame.GetChunk() != nil || s.stage == "terminal" && frame.GetTerminal() != nil
	if matches && s.panicked.CompareAndSwap(false, true) {
		panic("private SendMsg panic value")
	}

	return s.ServerStream.SendMsg(message)
}

func TestDetachedOutputSenderPanics(t *testing.T) {
	if os.Getenv("WIRE_TEST_SENDER_PANIC_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDetachedOutputSenderPanics$", "-test.timeout=40s")

		child.Env = append(os.Environ(), "WIRE_TEST_SENDER_PANIC_CHILD=1")
		if output, err := child.CombinedOutput(); err != nil {
			t.Fatalf("sender panic subprocess: %v\n%s", err, output)
		}

		return
	}

	for _, method := range []string{wirev1.ExecutionService_ReadExecutionOutput_FullMethodName, wirev1.DebugService_RunCommand_FullMethodName, wirev1.DebugService_WatchDebug_FullMethodName} {
		for _, stage := range []string{"envelope", "header", "chunk", "terminal"} {
			if stage == "envelope" && method == wirev1.ExecutionService_ReadExecutionOutput_FullMethodName {
				continue
			}

			t.Run(method+"/"+stage, func(t *testing.T) {
				output := &transportOutput{data: []byte("payload")}
				hosted := &transportDebugger{content: &api.Content{Data: output.data}}
				var panicked atomic.Bool
				f := newOutputTransportFixture(t, output, hosted, 1024, server.WithStreamInterceptors(func(host any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
					if info.FullMethod == method {
						stream = &panicSendStream{ServerStream: stream, stage: stage, panicked: &panicked}
					}

					return next(host, stream)
				}))
				var receive func() (*wirev1.OutputFrame, error)
				var release func()

				if method == wirev1.ExecutionService_ReadExecutionOutput_FullMethodName {
					receive, release = f.execution(t)
				} else {
					receive, release = f.debugger(t, method == wirev1.DebugService_WatchDebug_FullMethodName)
				}

				_, err := collectTransportFrames(receive)
				if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "private") {
					t.Fatalf("unsanitized send panic: %v", err)
				}

				details := status.Convert(err).Details()
				if len(details) != 1 {
					t.Fatalf("panic details=%v", details)
				}

				detail, ok := details[0].(*wirev1.ErrorDetail)
				if !ok || detail.GetCategory() != wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE {
					t.Fatalf("panic category=%v", details)
				}

				release()

				if method == wirev1.ExecutionService_ReadExecutionOutput_FullMethodName && output.closes.Load() != 1 || method != wirev1.ExecutionService_ReadExecutionOutput_FullMethodName && hosted.closes.Load() != 1 {
					t.Fatal("hosted resource did not close exactly once")
				}

				// Subsequent work under a one-resource quota verifies reclamation and server usability.
				if method == wirev1.ExecutionService_ReadExecutionOutput_FullMethodName {
					receive, release = f.execution(t)
				} else {
					receive, release = f.debugger(t, method == wirev1.DebugService_WatchDebug_FullMethodName)
				}

				data, err := collectTransportFrames(receive)
				release()

				if err != nil || string(data) != "payload" {
					t.Fatalf("server unusable after panic: data=%q error=%v", data, err)
				}
			})
		}
	}
}

func TestOversizedOutputHeaderPreservesTransportCategory(t *testing.T) {
	output := &transportOutput{metadata: api.Metadata{ContentType: strings.Repeat("x", 2048)}}
	f := newOutputTransportFixture(t, output, &transportDebugger{}, 1024)
	receive, release := f.execution(t)
	_, err := collectTransportFrames(receive)
	release()

	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "outbound message limit") {
		t.Fatalf("oversized header=%v", err)
	}

	if output.consumes.Load() != 0 || output.closes.Load() != 1 {
		t.Fatalf("consumes=%d closes=%d", output.consumes.Load(), output.closes.Load())
	}
}

func (s *responseCompression) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, outputMethodKey{}, info.FullMethodName)
}

func (s *responseCompression) HandleRPC(ctx context.Context, event stats.RPCStats) {
	header, ok := event.(*stats.InHeader)
	if !ok {
		return
	}

	switch ctx.Value(outputMethodKey{}) {
	case wirev1.ExecutionService_ReadExecutionOutput_FullMethodName, wirev1.DebugService_RunCommand_FullMethodName, wirev1.DebugService_WatchDebug_FullMethodName:
		s.mu.Lock()
		s.headers++
		s.compressed = s.compressed || header.Compression != "" && header.Compression != "identity"
		s.mu.Unlock()
	}
}

func (s *responseCompression) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (s *responseCompression) HandleConn(context.Context, stats.ConnStats) {}

func TestFramedOutputRejectsOversizedTerminalAndSemanticMessages(t *testing.T) {
	for _, method := range []string{"execution terminal", "command envelope", "watch envelope"} {
		t.Run(method, func(t *testing.T) {
			oversized := diagnostics.Diagnostics{{Message: strings.Repeat("portable diagnostic", 256)}}
			output := &transportOutput{data: []byte("prefix"), failure: oversized}
			hosted := &transportDebugger{content: &api.Content{Data: output.data}, failure: oversized}
			f := newOutputTransportFixture(t, output, hosted, 1024)
			var receive func() (*wirev1.OutputFrame, error)
			var release func()

			if method == "execution terminal" {
				receive, release = f.execution(t)
			} else {
				receive, release = f.debugger(t, method == "watch envelope")
			}

			data, err := collectTransportFrames(receive)
			release()

			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("oversized message=%v", err)
			}

			if method == "execution terminal" && string(data) != "prefix" {
				t.Fatalf("partial bytes=%q", data)
			}

			if method == "execution terminal" && output.closes.Load() != 1 || method != "execution terminal" && hosted.closes.Load() != 1 {
				t.Fatal("hosted cleanup did not settle once")
			}
		})
	}
}
