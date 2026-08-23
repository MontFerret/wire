package wire_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/ferret/v2"
	"github.com/MontFerret/ferret/v2/pkg/compiler"
	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
	"github.com/MontFerret/ferret/v2/pkg/source"
	"github.com/MontFerret/wire"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type integrationEnv struct {
	engine   *ferret.Engine
	server   *wire.Server
	listener *bufconn.Listener
	conn     *grpc.ClientConn
	client   *client.Client
	serveErr chan error
}

func newIntegrationEnv(t *testing.T, engineOptions ...ferret.Option) *integrationEnv {
	t.Helper()
	engine, err := ferret.New(engineOptions...)
	if err != nil {
		t.Fatal(err)
	}
	server, err := wire.NewServer(engine, wire.WithRuntimeIdentity(wire.RuntimeIdentity{
		Name: "test-host", Version: "1.2.3", InstanceID: "instance-1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(8 << 20)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(context.Background(), listener) }()

	conn, err := grpc.NewClient(
		"passthrough:///ferret-wire-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wireClient, err := client.New(ctx, conn, client.WithClientIdentity(client.ClientIdentity{Name: "tests", Version: "1"}))
	if err != nil {
		t.Fatal(err)
	}

	env := &integrationEnv{
		engine: engine, server: server, listener: listener, conn: conn, client: wireClient, serveErr: serveErr,
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = wireClient.Close(cleanupCtx)
		_ = server.Shutdown(cleanupCtx)
		_ = conn.Close()
		select {
		case serveErr := <-serveErr:
			if serveErr != nil {
				t.Errorf("Serve returned an error: %v", serveErr)
			}
		case <-cleanupCtx.Done():
			t.Errorf("Serve did not settle: %v", cleanupCtx.Err())
		}
		_ = engine.Close()
	})
	return env
}

func TestHandshakeCompileExecuteAndCanonicalOutput(t *testing.T) {
	env := newIntegrationEnv(t, ferret.WithFunctionsRegistrar(func(namespace ferretruntime.Namespace) {
		namespace.Function().A0().Add("HOST_VALUE", func(context.Context) (ferretruntime.Value, error) {
			return ferretruntime.NewString("from-host"), nil
		})
	}))
	info := env.client.RuntimeInfo()
	if info.APIIdentity != "ferret.wire.v1" || info.WireVersion == "" || info.FerretVersion != compiler.Version {
		t.Fatalf("unexpected runtime info: %#v", info)
	}
	if info.RuntimeIdentity == nil || info.RuntimeIdentity.Name != "test-host" || !info.Capabilities.Execution || !info.Capabilities.Debugging || !info.Capabilities.Cancellation {
		t.Fatalf("unexpected identity/capabilities: %#v", info)
	}

	plan, err := env.client.Compile(context.Background(), client.Source{
		Identity: "host.fql", Content: "RETURN {host: HOST_VALUE(), value: @value, nested: @nested}",
	}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(plan.Parameters)
	if !reflect.DeepEqual(plan.Parameters, []string{"nested", "value"}) {
		t.Fatalf("unexpected declared parameters: %v", plan.Parameters)
	}

	execution, err := env.client.Execute(context.Background(), plan.ID, map[string]any{
		"value": int64(42),
		"nested": map[string]any{
			"array": []any{true, "wire", nil},
		},
	}, client.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := env.client.WatchExecution(context.Background(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	var completed client.Execution
	var previous uint64
	for completed.State != client.ExecutionCompleted {
		event, recvErr := events.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if event.Sequence <= previous {
			t.Fatalf("non-monotonic event sequence: %d after %d", event.Sequence, previous)
		}
		previous = event.Sequence
		if event.Kind == client.ExecutionEventFailed || event.Kind == client.ExecutionEventCancelled {
			t.Fatalf("execution did not complete: %#v", event.Execution)
		}
		if event.Kind == client.ExecutionEventCompleted {
			completed = event.Execution
		}
	}
	if completed.Output == nil || completed.Output.ContentType != "application/json" {
		t.Fatalf("unexpected canonical output: %#v", completed.Output)
	}
	var decoded map[string]any
	if err := json.Unmarshal(completed.Output.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["host"] != "from-host" || decoded["value"] != float64(42) {
		t.Fatalf("unexpected output: %s", completed.Output.Data)
	}

	if err := env.client.ReleaseExecution(context.Background(), execution.ID); err != nil {
		t.Fatal(err)
	}
	if err := env.client.ReleaseExecution(context.Background(), execution.ID); err != nil {
		t.Fatalf("idempotent execution release failed: %v", err)
	}
	if err := env.client.ReleasePlan(context.Background(), plan.ID); err != nil {
		t.Fatal(err)
	}
}

func TestCompilationDiagnosticsAndConnectionIsolation(t *testing.T) {
	env := newIntegrationEnv(t)
	_, err := env.client.Compile(context.Background(), client.Source{Identity: "broken.fql", Content: "RETURN ("}, client.CompileOptions{})
	var wireErr *client.Error
	if !errors.As(err, &wireErr) || wireErr.Category != client.ErrorCompilation || wireErr.Code != codes.InvalidArgument {
		t.Fatalf("unexpected compilation error: %#v", err)
	}
	if len(wireErr.Diagnostics) == 0 || wireErr.Diagnostics[0].SourceIdentity != "broken.fql" || len(wireErr.Diagnostics[0].Spans) == 0 {
		t.Fatalf("missing structured diagnostics: %#v", wireErr)
	}

	plan, err := env.client.Compile(context.Background(), client.Source{Content: "RETURN 1"}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.New(context.Background(), env.conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close(context.Background()) })
	_, err = other.Execute(context.Background(), plan.ID, nil, client.ExecuteOptions{})
	if !errors.As(err, &wireErr) || wireErr.Category != client.ErrorPlanNotFound || wireErr.ResourceID != string(plan.ID) {
		t.Fatalf("cross-connection plan was not concealed: %#v", err)
	}
	if err := other.ReleasePlan(context.Background(), plan.ID); !errors.As(err, &wireErr) || wireErr.Category != client.ErrorPlanNotFound {
		t.Fatalf("cross-connection release was not concealed: %#v", err)
	}
	if err := env.client.ReleasePlan(context.Background(), client.PlanID("not-a-uuid")); !errors.As(err, &wireErr) || wireErr.Category != client.ErrorInvalidRequest {
		t.Fatalf("malformed ID was not rejected: %#v", err)
	}
}

func TestExecutionCancellationIsATerminalEvent(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	env := newIntegrationEnv(t, ferret.WithFunctionsRegistrar(func(namespace ferretruntime.Namespace) {
		namespace.Function().A0().Add("BLOCK", func(ctx context.Context) (ferretruntime.Value, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return ferretruntime.None, ctx.Err()
		})
	}))
	plan, err := env.client.Compile(context.Background(), client.Source{Content: "RETURN BLOCK()"}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := env.client.Execute(context.Background(), plan.ID, nil, client.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := env.client.WatchExecution(context.Background(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("host function did not start")
	}
	if _, err := env.client.CancelExecution(context.Background(), execution.ID); err != nil {
		t.Fatal(err)
	}
	for {
		event, recvErr := events.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if event.Kind == client.ExecutionEventCancelled {
			if event.Execution.State != client.ExecutionCancelled {
				t.Fatalf("unexpected cancellation snapshot: %#v", event.Execution)
			}
			break
		}
	}
}

func TestConcurrentExecutionsShareAPlan(t *testing.T) {
	env := newIntegrationEnv(t)
	plan, err := env.client.Compile(context.Background(), client.Source{Content: "RETURN @value"}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const count = 24
	start := make(chan struct{})
	errs := make(chan error, count)
	for index := range count {
		go func(value int) {
			<-start
			execution, executeErr := env.client.Execute(context.Background(), plan.ID, client.Parameters{"value": value}, client.ExecuteOptions{})
			if executeErr != nil {
				errs <- executeErr
				return
			}
			events, watchErr := env.client.WatchExecution(context.Background(), execution.ID)
			if watchErr != nil {
				errs <- watchErr
				return
			}
			for {
				event, receiveErr := events.Recv()
				if receiveErr != nil {
					errs <- receiveErr
					return
				}
				if event.Kind == client.ExecutionEventCompleted {
					var decoded int
					if jsonErr := json.Unmarshal(event.Execution.Output.Data, &decoded); jsonErr != nil || decoded != value {
						errs <- errors.New("concurrent execution returned the wrong output")
						return
					}
					errs <- nil
					return
				}
			}
		}(index)
	}
	close(start)
	for range count {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestExecutionFailureAndPlanReleaseCascade(t *testing.T) {
	env := newIntegrationEnv(t)
	failingPlan, err := env.client.Compile(context.Background(), client.Source{Identity: "failure.fql", Content: "RETURN 1 / 0"}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := env.client.Execute(context.Background(), failingPlan.ID, nil, client.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := env.client.WatchExecution(context.Background(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	for {
		event, recvErr := events.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if event.Kind == client.ExecutionEventFailed {
			if event.Execution.Failure == nil || len(event.Execution.Failure.Diagnostics) == 0 {
				t.Fatalf("execution failure lost diagnostics: %#v", event.Execution)
			}
			break
		}
	}

	started := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	blockingEnv := newIntegrationEnv(t, ferret.WithFunctionsRegistrar(func(namespace ferretruntime.Namespace) {
		namespace.Function().A0().Add("CASCADE_BLOCK", func(ctx context.Context) (ferretruntime.Value, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			close(finished)
			return ferretruntime.None, ctx.Err()
		})
	}))
	plan, err := blockingEnv.client.Compile(context.Background(), client.Source{Content: "RETURN CASCADE_BLOCK()"}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	running, err := blockingEnv.client.Execute(context.Background(), plan.ID, nil, client.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runningEvents, err := blockingEnv.client.WatchExecution(context.Background(), running.ID)
	if err != nil {
		t.Fatal(err)
	}
	if event, err := runningEvents.Recv(); err != nil || event.Kind != client.ExecutionEventStarted {
		t.Fatalf("execution watcher did not attach before cascade: %#v, %v", event, err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("cascaded execution did not start")
	}
	if err := blockingEnv.client.ReleasePlan(context.Background(), plan.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("plan release did not settle its execution")
	}
	for {
		event, recvErr := runningEvents.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if event.Kind == client.ExecutionEventCancelled {
			break
		}
	}
	if err := blockingEnv.client.ReleasePlan(context.Background(), plan.ID); err != nil {
		t.Fatalf("repeated plan release failed: %v", err)
	}
}

func TestDebugPreStartBreakpointsInspectionAndCompletion(t *testing.T) {
	env := newIntegrationEnv(t)
	plan, err := env.client.Compile(context.Background(), client.Source{
		Identity: "debug.fql", Content: "LET x = 1\n\nVAR y = @input\ny = y + x\nRETURN y",
	}, client.CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	session, err := env.client.OpenDebugSession(context.Background(), plan.ID, map[string]any{"input": 2}, client.DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if session.State != client.DebugCreated {
		t.Fatalf("unexpected pre-start state: %#v", session)
	}
	breakpoints, err := env.client.SetBreakpoints(context.Background(), session.ID, "debug.fql", []client.BreakpointLocation{{Line: 2, Column: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(breakpoints) != 1 || !breakpoints[0].Verified || breakpoints[0].Line != 3 {
		t.Fatalf("unexpected breakpoint binding: %#v", breakpoints)
	}
	events, err := env.client.WatchDebug(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.StartDebug(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	entry := waitDebugStop(t, events)
	if entry.StopReason != client.DebugStopEntry || entry.Location == nil || entry.Location.Line != 1 {
		t.Fatalf("unexpected entry stop: %#v", entry)
	}
	if _, err := env.client.Continue(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	stopped := waitDebugStop(t, events)
	if stopped.StopReason != client.DebugStopBreakpoint || len(stopped.HitBreakpointIDs) != 1 {
		t.Fatalf("unexpected breakpoint stop: %#v", stopped)
	}
	frames, err := env.client.StackTrace(context.Background(), session.ID)
	if err != nil || len(frames) == 0 || frames[0].Index != 0 {
		t.Fatalf("unexpected frames: %#v, %v", frames, err)
	}
	scopes, err := env.client.Scopes(context.Background(), session.ID, 0)
	if err != nil || len(scopes) != 2 {
		t.Fatalf("unexpected scopes: %#v, %v", scopes, err)
	}
	value, err := env.client.Evaluate(context.Background(), session.ID, 0, "x + @input")
	if err != nil || value.Display != "3" {
		t.Fatalf("unexpected evaluation: %#v, %v", value, err)
	}
	if _, err := env.client.Continue(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	for {
		event, recvErr := events.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if event.Kind == client.DebugEventCompleted {
			if event.Session.Output == nil || string(event.Session.Output.Data) != "3" {
				t.Fatalf("unexpected debug output: %#v", event.Session.Output)
			}
			break
		}
	}
}

func TestDebugStepCommandsAndStaleValueReferences(t *testing.T) {
	env := newIntegrationEnv(t)
	udfPlan, err := env.client.Compile(context.Background(), client.Source{Identity: "udf.fql", Content: `FUNC add(a) {
  LET b = a + 1
  RETURN b
}
LET x = add(2)
RETURN x`}, client.CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	session, err := env.client.OpenDebugSession(context.Background(), udfPlan.ID, nil, client.DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := env.client.WatchDebug(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.StartDebug(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if entry := waitDebugStop(t, events); entry.Location == nil || entry.Location.Line != 5 {
		t.Fatalf("unexpected UDF entry: %#v", entry)
	}
	if _, err := env.client.StepIn(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	inside := waitDebugStop(t, events)
	if inside.Location == nil || inside.Location.Line != 2 {
		t.Fatalf("step-in did not enter UDF: %#v", inside)
	}
	frames, err := env.client.StackTrace(context.Background(), session.ID)
	if err != nil || len(frames) != 2 {
		t.Fatalf("unexpected UDF frames: %#v, %v", frames, err)
	}
	if _, err := env.client.StepOut(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if outside := waitDebugStop(t, events); outside.Location == nil || outside.Location.Line != 6 {
		t.Fatalf("step-out did not return to main: %#v", outside)
	}

	nextSession, err := env.client.OpenDebugSession(context.Background(), udfPlan.ID, nil, client.DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	nextEvents, err := env.client.WatchDebug(context.Background(), nextSession.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.StartDebug(context.Background(), nextSession.ID); err != nil {
		t.Fatal(err)
	}
	waitDebugStop(t, nextEvents)
	if _, err := env.client.Next(context.Background(), nextSession.ID); err != nil {
		t.Fatal(err)
	}
	if stopped := waitDebugStop(t, nextEvents); stopped.Location == nil || stopped.Location.Line != 6 {
		t.Fatalf("next did not step over UDF: %#v", stopped)
	}
	if _, err := env.client.StopDebug(context.Background(), nextSession.ID); err != nil {
		t.Fatal(err)
	}

	objectPlan, err := env.client.Compile(context.Background(), client.Source{Identity: "object.fql", Content: "LET obj = {nested: [1, 2]}\nLET x = 1\nRETURN obj"}, client.CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	objectSession, err := env.client.OpenDebugSession(context.Background(), objectPlan.ID, nil, client.DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	objectEvents, err := env.client.WatchDebug(context.Background(), objectSession.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.StartDebug(context.Background(), objectSession.ID); err != nil {
		t.Fatal(err)
	}
	waitDebugStop(t, objectEvents)
	if _, err := env.client.Next(context.Background(), objectSession.ID); err != nil {
		t.Fatal(err)
	}
	waitDebugStop(t, objectEvents)
	scopes, err := env.client.Scopes(context.Background(), objectSession.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	var reference uint64
	for _, scope := range scopes {
		for _, variable := range scope.Variables {
			if variable.Name == "obj" {
				reference = variable.Value.Reference
			}
		}
	}
	if reference == 0 {
		t.Fatalf("expandable object reference was not exposed: %#v", scopes)
	}
	if variables, err := env.client.Variables(context.Background(), objectSession.ID, reference); err != nil || len(variables) != 1 {
		t.Fatalf("object expansion failed: %#v, %v", variables, err)
	}
	if _, err := env.client.Next(context.Background(), objectSession.ID); err != nil {
		t.Fatal(err)
	}
	waitDebugStop(t, objectEvents)
	_, err = env.client.Variables(context.Background(), objectSession.ID, reference)
	var wireErr *client.Error
	if !errors.As(err, &wireErr) || wireErr.Code != codes.NotFound || wireErr.Category != client.ErrorValueReferenceNotFound {
		t.Fatalf("stale value reference was not mapped to NotFound: %#v", err)
	}
}

func TestDebugRuntimeErrorStopAndTermination(t *testing.T) {
	env := newIntegrationEnv(t)
	plan, err := env.client.Compile(context.Background(), client.Source{Identity: "error.fql", Content: "LET x = 7\nRETURN x / 0"}, client.CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	session, err := env.client.OpenDebugSession(context.Background(), plan.ID, nil, client.DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := env.client.WatchDebug(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.StartDebug(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	waitDebugStop(t, events)
	if _, err := env.client.Continue(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	stopped := waitDebugStop(t, events)
	if stopped.StopReason != client.DebugStopRuntimeError || stopped.Failure == nil || len(stopped.Failure.Diagnostics) == 0 {
		t.Fatalf("runtime error was not preserved as an inspectable stop: %#v", stopped)
	}
	if _, err := env.client.Continue(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	for {
		event, recvErr := events.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if event.Kind == client.DebugEventFailed || event.Kind == client.DebugEventTerminated {
			break
		}
	}
}

func TestDebugPause(t *testing.T) {
	env := newIntegrationEnv(t)
	plan, err := env.client.Compile(context.Background(), client.Source{Identity: "pause.fql", Content: "LET obj = {b: 2, a: 1}\nRETURN obj"}, client.CompileOptions{Debuggable: true})
	if err != nil {
		t.Fatal(err)
	}
	session, err := env.client.OpenDebugSession(context.Background(), plan.ID, nil, client.DebugSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := env.client.WatchDebug(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.StartDebug(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	waitDebugStop(t, events)
	if _, err := env.client.Pause(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.client.Continue(context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	stopped := waitDebugStop(t, events)
	if stopped.StopReason != client.DebugStopPause {
		t.Fatalf("unexpected pause stop: %#v", stopped)
	}
}

func waitDebugStop(t *testing.T, events *client.DebugEvents) client.DebugSession {
	t.Helper()
	for {
		event, err := events.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if event.Kind == client.DebugEventStopped {
			return event.Session
		}
	}
}

func TestInboundMessageLimitAndBorrowedEngine(t *testing.T) {
	env := newIntegrationEnv(t)
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runtimeClient := wirev1.NewRuntimeServiceClient(env.conn)
	stream, err := runtimeClient.Connect(streamCtx, &wirev1.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}
	handshake, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	_, err = wirev1.NewPlanServiceClient(env.conn).Compile(context.Background(), &wirev1.CompileRequest{
		ConnectionId: handshake.GetOpened().GetConnectionId(),
		Source:       &wirev1.Source{Content: "RETURN 1 //" + string(make([]byte, 4<<20))},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unexpected oversized-message result: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := env.client.Close(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if err := env.server.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	output, err := env.engine.Run(context.Background(), ferretSource("RETURN 7"))
	if err != nil || string(output.Content) != "7" {
		t.Fatalf("server closed the borrowed engine: %q, %v", output.Content, err)
	}
}

func TestConnectCancellationReleasesOwnedExecution(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	env := newIntegrationEnv(t, ferret.WithFunctionsRegistrar(func(namespace ferretruntime.Namespace) {
		namespace.Function().A0().Add("CONNECTION_BLOCK", func(ctx context.Context) (ferretruntime.Value, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			close(finished)
			return ferretruntime.None, ctx.Err()
		})
	}))
	connectCtx, cancelConnect := context.WithCancel(context.Background())
	runtimeClient := wirev1.NewRuntimeServiceClient(env.conn)
	stream, err := runtimeClient.Connect(connectCtx, &wirev1.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}
	handshake, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	connectionID := handshake.GetOpened().GetConnectionId()
	compiled, err := wirev1.NewPlanServiceClient(env.conn).Compile(context.Background(), &wirev1.CompileRequest{
		ConnectionId: connectionID,
		Source:       &wirev1.Source{Content: "RETURN CONNECTION_BLOCK()"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = wirev1.NewExecutionServiceClient(env.conn).Execute(context.Background(), &wirev1.ExecuteRequest{
		ConnectionId: connectionID,
		PlanId:       compiled.GetPlan().GetId(),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("connection-scoped execution did not start")
	}
	cancelConnect()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("Connect cancellation did not release the execution")
	}
	_, err = wirev1.NewPlanServiceClient(env.conn).Compile(context.Background(), &wirev1.CompileRequest{
		ConnectionId: connectionID,
		Source:       &wirev1.Source{Content: "RETURN 1"},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("cancelled logical connection remained visible: %v", err)
	}
}

func ferretSource(content string) *source.Source {
	return source.New("after-shutdown.fql", content)
}
