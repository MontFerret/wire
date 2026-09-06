package server_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server"
)

func TestProtocolConnectionsHaveIndependentStoresAndLimits(t *testing.T) {
	limits := server.DefaultLimits()
	limits.MaxPlansPerConnection = 1
	limits.MaxSessionsPerConnection = 1
	limits.MaxExecutionsPerConnection = 1
	limits.MaxDebugSessionsPerConnection = 1
	env := newIntegrationEnv(t, &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
		return &contractPlan{
			newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
				return &apiSessionSpy{}, nil
			},
			newDebugSession: func(context.Context, apiSessionOptions) (debugger.Session, error) {
				return &unstartedProtocolDebugger{}, nil
			},
		}, nil
	}}, server.WithLimits(limits))
	ctx := testContext(t)
	runtimes := wirev1.NewRuntimeServiceClient(env.conn)
	plans := wirev1.NewPlanServiceClient(env.conn)
	sessions := wirev1.NewSessionServiceClient(env.conn)
	executions := wirev1.NewExecutionServiceClient(env.conn)
	debuggers := wirev1.NewDebugServiceClient(env.conn)
	type resources struct {
		connection *wirev1.ConnectionId
		plan       *wirev1.PlanId
		session    *wirev1.SessionId
		execution  *wirev1.ExecutionId
		debug      *wirev1.DebugSessionId
	}
	var owners [2]resources
	for i := range owners {
		connectCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := runtimes.Connect(connectCtx, &wirev1.ConnectRequest{})
		if err != nil {
			t.Fatal(err)
		}

		handshake, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}

		owner := &owners[i]
		owner.connection = handshake.GetConnectionId()
		defer func() {
			if _, err := runtimes.CloseConnection(testContext(t), &wirev1.CloseConnectionRequest{ConnectionId: owner.connection}); err != nil {
				t.Error(err)
			}
		}()
		compile := &wirev1.CompileDebugRequest{ConnectionId: owner.connection, Source: &wirev1.Source{Content: "RETURN 1"}}

		plan, err := plans.CompileDebug(ctx, compile)
		if err != nil {
			t.Fatalf("connection %d cannot allocate its own plan: %v", i, err)
		}

		owner.plan = plan.GetPlan().GetId()

		if _, err := plans.CompileDebug(ctx, compile); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("plan limit: %v", err)
		}

		createSession := &wirev1.CreateSessionRequest{ConnectionId: owner.connection, PlanId: owner.plan}

		session, err := sessions.CreateSession(ctx, createSession)
		if err != nil {
			t.Fatalf("connection %d cannot allocate its own session: %v", i, err)
		}

		owner.session = session.GetSession().GetId()

		if _, err := sessions.CreateSession(ctx, createSession); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("session limit: %v", err)
		}

		run := &wirev1.RunSessionRequest{ConnectionId: owner.connection, SessionId: owner.session}

		execution, err := executions.RunSession(ctx, run)
		if err != nil {
			t.Fatalf("connection %d cannot allocate its own execution: %v", i, err)
		}

		owner.execution = execution.GetExecution().GetId()

		if _, err := executions.RunSession(ctx, run); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("execution limit: %v", err)
		}

		createDebug := &wirev1.CreateDebugSessionRequest{ConnectionId: owner.connection, PlanId: owner.plan}

		debug, err := debuggers.CreateDebugSession(ctx, createDebug)
		if err != nil {
			t.Fatalf("connection %d cannot allocate its own debugger: %v", i, err)
		}

		owner.debug = debug.GetSession().GetId()

		if _, err := debuggers.CreateDebugSession(ctx, createDebug); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("debugger limit: %v", err)
		}
	}

	releaseChildren := func(connection *wirev1.ConnectionId, target resources) {
		t.Helper()

		if _, err := sessions.ReleaseSession(ctx, &wirev1.ReleaseSessionRequest{ConnectionId: connection, SessionId: target.session}); status.Code(err) != codes.NotFound {
			t.Fatalf("foreign or stale session was accessible: %v", err)
		}

		if _, err := executions.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{ConnectionId: connection, ExecutionId: target.execution}); status.Code(err) != codes.NotFound {
			t.Fatalf("foreign or stale execution was accessible: %v", err)
		}

		if _, err := debuggers.ReleaseDebugSession(ctx, &wirev1.ReleaseDebugSessionRequest{ConnectionId: connection, DebugSessionId: target.debug}); status.Code(err) != codes.NotFound {
			t.Fatalf("foreign or stale debugger was accessible: %v", err)
		}
	}
	releaseChildren(owners[1].connection, owners[0])

	if _, err := plans.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{ConnectionId: owners[1].connection, PlanId: owners[0].plan}); status.Code(err) != codes.NotFound {
		t.Fatalf("foreign plan was accessible: %v", err)
	}

	if _, err := plans.ReleasePlan(ctx, &wirev1.ReleasePlanRequest{ConnectionId: owners[0].connection, PlanId: owners[0].plan}); err != nil {
		t.Fatal(err)
	}

	releaseChildren(owners[0].connection, owners[0])

	if _, err := executions.ReleaseExecution(ctx, &wirev1.ReleaseExecutionRequest{ConnectionId: owners[1].connection, ExecutionId: owners[1].execution}); err != nil {
		t.Fatalf("other connection lost its execution: %v", err)
	}

	if _, err := executions.RunSession(ctx, &wirev1.RunSessionRequest{ConnectionId: owners[1].connection, SessionId: owners[1].session}); err != nil {
		t.Fatalf("other connection lost its reusable session: %v", err)
	}
}
