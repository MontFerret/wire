package server_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

// Termination before Start only needs Close. Any unexpected debugger operation
// reaches the embedded nil interface and fails the hosted implementation boundary.
type unstartedProtocolDebugger struct {
	debugger.Session
	closes atomic.Int64
}

func (d *unstartedProtocolDebugger) Close() error {
	d.closes.Add(1)

	return nil
}

// Execute, CancelExecution, and Terminate remain protocol operations even though
// the handwritten client no longer exposes a second resource programming model.
func TestProtocolResourceOperationsRemainAvailable(t *testing.T) {
	started := make(chan struct{})
	var sessionCloses atomic.Int64
	debug := &unstartedProtocolDebugger{}
	plan := &contractPlan{
		newSession: func(_ context.Context, options apiSessionOptions) (api.Session, error) {
			if options.contentType != "text/plain" || options.params["input"] != int64(7) {
				t.Errorf("Execute lost session options: %+v", options)
			}

			return &apiSessionSpy{
				run: func(ctx context.Context) (api.Output, error) {
					close(started)
					<-ctx.Done()

					return api.Output{}, ctx.Err()
				},
				close: func() error {
					sessionCloses.Add(1)

					return nil
				},
			}, nil
		},
		newDebugSession: func(context.Context, apiSessionOptions) (debugger.Session, error) {
			return debug, nil
		},
	}
	env := newIntegrationEnv(t, &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
		return plan, nil
	}})
	ctx := testContext(t)
	connectCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runtimeRPC := wirev1.NewRuntimeServiceClient(env.conn)
	stream, err := runtimeRPC.Connect(connectCtx, &wirev1.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}

	handshake, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}

	connectionID := handshake.GetConnectionId()
	defer func() {
		if _, err := runtimeRPC.CloseConnection(testContext(t), &wirev1.CloseConnectionRequest{ConnectionId: connectionID}); err != nil {
			t.Error(err)
		}
	}()
	planRPC := wirev1.NewPlanServiceClient(env.conn)
	compiled, err := planRPC.CompileDebug(ctx, &wirev1.CompileDebugRequest{ConnectionId: connectionID, Source: &wirev1.Source{Content: "RETURN @input"}})
	if err != nil {
		t.Fatal(err)
	}

	planID := compiled.GetPlan().GetId()
	executionRPC := wirev1.NewExecutionServiceClient(env.conn)
	created, err := executionRPC.Execute(ctx, &wirev1.ExecuteRequest{
		ConnectionId: connectionID, PlanId: planID, OutputContentType: "text/plain",
		Parameters: &wirev1.Parameters{Values: map[string]*wirev1.Value{"input": {Value: &wirev1.Value_IntegerValue{IntegerValue: 7}}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("Execute did not reach the hosted session")
	}

	executionID := created.GetExecution().GetId()
	if _, err := executionRPC.CancelExecution(ctx, &wirev1.CancelExecutionRequest{ConnectionId: connectionID, ExecutionId: executionID}); err != nil {
		t.Fatal(err)
	}

	watch, err := executionRPC.WatchExecution(ctx, &wirev1.WatchExecutionRequest{ConnectionId: connectionID, ExecutionId: executionID})
	if err != nil {
		t.Fatal(err)
	}

	event, err := watch.Recv()
	if err != nil || event.GetExecution().GetState() != wirev1.ExecutionState_EXECUTION_STATE_CANCELLED {
		t.Fatalf("CancelExecution lost its terminal event: %v, %v", event, err)
	}

	if _, err := executionRPC.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{ConnectionId: connectionID, ExecutionId: executionID}); err != nil {
		t.Fatal(err)
	}

	debugRPC := wirev1.NewDebugServiceClient(env.conn)
	debugCreated, err := debugRPC.CreateDebugSession(ctx, &wirev1.CreateDebugSessionRequest{ConnectionId: connectionID, PlanId: planID})
	if err != nil {
		t.Fatal(err)
	}

	debugID := debugCreated.GetSession().GetId()
	if _, err := debugRPC.Terminate(ctx, &wirev1.TerminateRequest{ConnectionId: connectionID, DebugSessionId: debugID}); err != nil {
		t.Fatal(err)
	}

	debugWatch, err := debugRPC.WatchDebug(ctx, &wirev1.WatchDebugRequest{ConnectionId: connectionID, DebugSessionId: debugID})
	if err != nil {
		t.Fatal(err)
	}

	debugEvent, err := debugWatch.Recv()
	if err != nil || debugEvent.GetKind() != wirev1.DebugEventKind_DEBUG_EVENT_KIND_TERMINATED {
		t.Fatalf("Terminate lost its terminal event: %v, %v", debugEvent, err)
	}

	if _, err := debugRPC.ReleaseDebugSession(ctx, &wirev1.ReleaseDebugSessionRequest{ConnectionId: connectionID, DebugSessionId: debugID}); err != nil {
		t.Fatal(err)
	}

	if _, err := planRPC.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{ConnectionId: connectionID, PlanId: planID}); err != nil {
		t.Fatal(err)
	}

	if sessionCloses.Load() != 1 || debug.closes.Load() != 1 {
		t.Fatalf("protocol cleanup counts: session=%d debugger=%d", sessionCloses.Load(), debug.closes.Load())
	}
}
