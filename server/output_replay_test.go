package server_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/outputstream"
	"github.com/MontFerret/wire/server"
)

type (
	completionDebugger struct {
		debugger.Session
		content *api.Content
	}
	debugOutputFixture struct {
		ctx        context.Context
		client     wirev1.DebugServiceClient
		connection *wirev1.ConnectionId
		plan       *wirev1.PlanId
		session    *wirev1.DebugSessionId
	}
)

func (d *completionDebugger) Start(context.Context) (*debugger.Event, error) {
	return &debugger.Event{Reason: debugger.ReasonCompleted, Output: d.content}, nil
}

func (d *completionDebugger) Close() error { return nil }

func newDebugOutputFixture(t testing.TB, data []byte, limit int) *debugOutputFixture {
	t.Helper()
	plan := &contractPlan{newDebugSession: func(context.Context, apiSessionOptions) (debugger.Session, error) {
		return &completionDebugger{content: &api.Content{Metadata: api.Metadata{Length: int64(len(data)), LengthKnown: true}, Data: data}}, nil
	}}
	limits := server.DefaultLimits()
	limits.MaxOutboundMessageBytes = limit
	env := newIntegrationEnv(t, &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) { return plan, nil }}, server.WithLimits(limits))
	ctx, cancel := context.WithCancel(testContext(t))
	t.Cleanup(cancel)

	connection, err := wirev1.NewRuntimeServiceClient(env.conn).Connect(ctx, &wirev1.ConnectRequest{OutputFramingVersion: outputstream.Version})
	if err != nil {
		t.Fatal(err)
	}

	header, err := connection.Recv()
	if err != nil {
		t.Fatal(err)
	}

	compiled, err := wirev1.NewPlanServiceClient(env.conn).CompileDebug(ctx, &wirev1.CompileDebugRequest{ConnectionId: header.GetConnectionId(), Source: &wirev1.Source{Content: "query"}})
	if err != nil {
		t.Fatal(err)
	}

	remote := wirev1.NewDebugServiceClient(env.conn)

	created, err := remote.CreateDebugSession(ctx, &wirev1.CreateDebugSessionRequest{ConnectionId: header.GetConnectionId(), PlanId: compiled.GetPlan().GetId()})
	if err != nil {
		t.Fatal(err)
	}

	return &debugOutputFixture{ctx: ctx, client: remote, connection: header.GetConnectionId(), plan: compiled.GetPlan().GetId(), session: created.GetSession().GetId()}
}

func (f *debugOutputFixture) watch(t testing.TB) wirev1.DebugService_WatchDebugClient {
	t.Helper()

	stream, err := f.client.WatchDebug(f.ctx, &wirev1.WatchDebugRequest{ConnectionId: f.connection, DebugSessionId: f.session})
	if err != nil {
		t.Fatal(err)
	}

	return stream
}

func (f *debugOutputFixture) start(t testing.TB, payload []byte) {
	t.Helper()

	stream, err := f.client.RunCommand(f.ctx, &wirev1.RunCommandRequest{ConnectionId: f.connection, DebugSessionId: f.session, Command: wirev1.DebugCommand_DEBUG_COMMAND_START})
	if err != nil {
		t.Fatal(err)
	}

	envelope, err := stream.Recv()
	if err != nil || envelope.GetResult().GetEvent().GetOutput().GetMetadata() == nil {
		t.Fatalf("command envelope=%v error=%v", envelope, err)
	}

	var decoder outputstream.Decoder
	received := 0
	for {
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		data, chunk, terminal, err := decoder.Accept(response.GetOutputFrame())
		if err != nil || response.GetResult() != nil {
			t.Fatalf("frame=%v error=%v", response, err)
		}

		if chunk {
			if !bytes.Equal(data, payload[received:received+len(data)]) {
				t.Fatal("command payload changed")
			}

			received += len(data)
		}

		if terminal != nil {
			break
		}
	}

	if received != len(payload) {
		t.Fatalf("received=%d", received)
	}

	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("extra command payload: %v", err)
	}
}

func (f *debugOutputFixture) completion(t testing.TB, stream wirev1.DebugService_WatchDebugClient, payload []byte) uint64 {
	t.Helper()
	var sequence uint64
	for {
		event, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if event.GetSequence() <= sequence || event.GetOutputFrame() != nil {
			t.Fatal("invalid semantic event ordering")
		}

		sequence = event.GetSequence()
		if event.GetSession().GetOutput() != nil {
			break
		}
	}

	var decoder outputstream.Decoder
	received := 0
	for {
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		if response.GetSequence() != 0 || response.GetSession() != nil {
			t.Fatal("output chunks became semantic events")
		}

		data, chunk, terminal, err := decoder.Accept(response.GetOutputFrame())
		if err != nil {
			t.Fatal(err)
		}

		if chunk {
			if received+len(data) > len(payload) || !bytes.Equal(data, payload[received:received+len(data)]) {
				t.Fatal("watch payload changed or repeated")
			}

			received += len(data)
		}

		if terminal != nil {
			break
		}
	}

	if received != len(payload) {
		t.Fatalf("received=%d", received)
	}

	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("extra watch payload: %v", err)
	}

	return sequence
}

func TestLargeDebugWatchAndReplayDoNotWaitForSlowObserver(t *testing.T) {
	payload := bytes.Repeat([]byte("replay"), 1<<20)
	f := newDebugOutputFixture(t, payload, 1024)

	slow := f.watch(t)
	if _, err := slow.Recv(); err != nil {
		t.Fatal(err)
	}

	fast := f.watch(t)
	f.start(t, payload)

	sequence := f.completion(t, fast, payload)
	if replay := f.completion(t, f.watch(t), payload); replay != sequence {
		t.Fatalf("replay sequence=%d want=%d", replay, sequence)
	}

	if _, err := f.client.ReleaseDebugSession(f.ctx, &wirev1.ReleaseDebugSessionRequest{ConnectionId: f.connection, DebugSessionId: f.session}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDebuggerOutputReplay(b *testing.B) {
	for _, test := range []struct {
		name string
		size int
	}{{"small", 64}, {"large", 8 << 20}} {
		b.Run(test.name, func(b *testing.B) {
			payload := make([]byte, test.size)
			f := newDebugOutputFixture(b, payload, 4<<20)
			f.start(b, payload)
			b.SetBytes(int64(test.size))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				f.completion(b, f.watch(b), payload)
			}
		})
	}
}

func BenchmarkDebuggerOutputFanout(b *testing.B) {
	payload := make([]byte, 8<<20)

	f := newDebugOutputFixture(b, payload, 4<<20)
	if _, err := f.client.ReleaseDebugSession(f.ctx, &wirev1.ReleaseDebugSessionRequest{ConnectionId: f.connection, DebugSessionId: f.session}); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		created, err := f.client.CreateDebugSession(f.ctx, &wirev1.CreateDebugSessionRequest{ConnectionId: f.connection, PlanId: f.plan})
		if err != nil {
			b.Fatal(err)
		}

		f.session = created.GetSession().GetId()
		watchers := make([]wirev1.DebugService_WatchDebugClient, 4)
		for i := range watchers {
			watchers[i] = f.watch(b)
			if _, err := watchers[i].Recv(); err != nil {
				b.Fatal(err)
			}
		}

		f.start(b, payload)
		for _, watcher := range watchers {
			f.completion(b, watcher, payload)
		}

		f.completion(b, f.watch(b), payload)

		if _, err := f.client.ReleaseDebugSession(f.ctx, &wirev1.ReleaseDebugSessionRequest{ConnectionId: f.connection, DebugSessionId: f.session}); err != nil {
			b.Fatal(err)
		}
	}
}
