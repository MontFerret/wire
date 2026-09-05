package server_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

type (
	allocationOperation struct {
		name          string
		method        string
		release       string
		parentRelease string
	}

	runtimeAllocationFixture struct {
		t                  *testing.T
		env                *integrationEnv
		gate               *allocationResponseGate
		remote             *client.Runtime
		plan               api.Plan
		session            api.Session
		operation          allocationOperation
		mu                 sync.Mutex
		plans              []*contractPlan
		sessions           []*contractSession
		debuggers          []*contractDebugger
		expectedCloseError error
	}
)

func allocationOperations() []allocationOperation {
	return []allocationOperation{
		{"compile", wirev1.PlanService_Compile_FullMethodName, wirev1.PlanService_ReleasePlan_FullMethodName, wirev1.RuntimeService_CloseConnection_FullMethodName},
		{"compile debug", wirev1.PlanService_CompileDebug_FullMethodName, wirev1.PlanService_ReleasePlan_FullMethodName, wirev1.RuntimeService_CloseConnection_FullMethodName},
		{"session", wirev1.SessionService_CreateSession_FullMethodName, wirev1.SessionService_ReleaseSession_FullMethodName, wirev1.PlanService_ReleasePlan_FullMethodName},
		{"debug session", wirev1.DebugService_CreateDebugSession_FullMethodName, wirev1.DebugService_ReleaseDebugSession_FullMethodName, wirev1.PlanService_ReleasePlan_FullMethodName},
		{"session run", wirev1.ExecutionService_RunSession_FullMethodName, wirev1.ExecutionService_ReleaseExecution_FullMethodName, wirev1.SessionService_ReleaseSession_FullMethodName},
		{"runtime run", wirev1.RuntimeService_Run_FullMethodName, wirev1.ExecutionService_ReleaseExecution_FullMethodName, wirev1.RuntimeService_CloseConnection_FullMethodName},
	}
}

func newRuntimeAllocationFixture(t *testing.T, operation allocationOperation) *runtimeAllocationFixture {
	t.Helper()
	f := &runtimeAllocationFixture{t: t, operation: operation}
	hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
		plan := &contractPlan{
			newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
				session := &contractSession{}
				f.mu.Lock()
				f.sessions = append(f.sessions, session)
				f.mu.Unlock()

				return session, nil
			},
			newDebugSession: func(context.Context, apiSessionOptions) (debugger.Session, error) {
				session := newContractDebugger()
				f.mu.Lock()
				f.debuggers = append(f.debuggers, session)
				f.mu.Unlock()

				return session, nil
			},
		}
		f.mu.Lock()
		f.plans = append(f.plans, plan)
		f.mu.Unlock()

		return plan, nil
	}}
	f.env = newIntegrationEnv(t, hosted)
	f.gate = &allocationResponseGate{ClientConnInterface: f.env.conn, calls: make(map[string]int), failures: make(map[string]error)}
	var err error
	f.remote, err = client.NewRuntime(testContext(t), f.gate)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := f.remote.Close(); err != nil && !errors.Is(err, f.expectedCloseError) {
			t.Errorf("close fixture Runtime: %v", err)
		}
		f.assertAllClosed()
		hosted.mu.Lock()
		defer hosted.mu.Unlock()
		if hosted.closeCalls != 0 {
			t.Errorf("closed borrowed Runtime %d times", hosted.closeCalls)
		}
	})

	switch operation.name {
	case "session", "debug session", "session run":
		f.plan, err = f.remote.CompileDebug(testContext(t), api.Source{Content: "RETURN 1"})
		if err != nil {
			t.Fatal(err)
		}
	}

	if operation.name == "session run" {
		f.session, err = f.plan.NewSession(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
	}

	return f
}

func (f *runtimeAllocationFixture) allocate(ctx context.Context, cancelInOption context.CancelFunc) (func() error, error) {
	var planOptions []api.PlanOption
	var sessionOptions []api.SessionOption
	if cancelInOption != nil {
		planOptions = []api.PlanOption{func(api.PlanOptions) error { cancelInOption(); return nil }}
		sessionOptions = []api.SessionOption{func(api.SessionOptions) error { cancelInOption(); return nil }}
	}

	switch f.operation.name {
	case "compile", "compile debug":
		compile := f.remote.Compile
		if f.operation.name == "compile debug" {
			compile = f.remote.CompileDebug
		}

		plan, err := compile(ctx, api.Source{Content: "RETURN 1"}, planOptions...)
		if err != nil {
			return nil, err
		}

		return plan.Close, nil
	case "session":
		session, err := f.plan.NewSession(ctx, sessionOptions...)
		if err != nil {
			return nil, err
		}

		return session.Close, nil
	case "debug session":
		session, err := f.plan.NewDebugSession(ctx, sessionOptions...)
		if err != nil {
			return nil, err
		}

		return session.Close, nil
	case "session run":
		_, err := f.session.Run(ctx)

		return nil, err
	default:
		_, err := f.remote.Run(ctx, api.Source{Content: "RETURN 1"}, sessionOptions...)

		return nil, err
	}
}

func (f *runtimeAllocationFixture) awaitCommitted() {
	f.t.Helper()
	select {
	case <-f.gate.committed:
	case <-time.After(5 * time.Second):
		f.t.Fatal("allocation did not commit")
	}
}

func (f *runtimeAllocationFixture) awaitResult(result <-chan error) error {
	f.t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		f.t.Fatal("allocation or cleanup did not settle")

		return nil
	}
}

func (f *runtimeAllocationFixture) assertAllClosed() {
	f.t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		f.mu.Lock()
		settled := true
		for _, plan := range f.plans {
			plan.mu.Lock()
			settled = settled && plan.closeCalls == 1
			plan.mu.Unlock()
		}
		for _, session := range f.sessions {
			_, closes := session.counts()
			settled = settled && closes == 1
		}
		for _, session := range f.debuggers {
			session.mu.Lock()
			settled = settled && session.closeCalls == 1
			session.mu.Unlock()
		}
		f.mu.Unlock()
		if settled {
			return
		}

		select {
		case <-tick.C:
		case <-deadline.C:
			f.t.Error("hosted resources were leaked or closed more than once")

			return
		}
	}
}

func (f *runtimeAllocationFixture) assertNarrowParentClosed() {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.operation.name != "session run" {
		f.plans[0].mu.Lock()
		closes := f.plans[0].closeCalls
		f.plans[0].mu.Unlock()
		if closes != 1 {
			f.t.Fatalf("narrow reclamation did not close its hosted Plan: closes=%d", closes)
		}
	}

	switch f.operation.name {
	case "session run":
		if _, closes := f.sessions[0].counts(); closes != 1 {
			f.t.Fatalf("narrow reclamation did not close its hosted Session: closes=%d", closes)
		}
	case "session":
		if _, closes := f.sessions[len(f.sessions)-1].counts(); closes != 1 {
			f.t.Fatalf("Plan reclamation did not close the unknown hosted Session: closes=%d", closes)
		}
	case "debug session":
		f.debuggers[0].mu.Lock()
		closes := f.debuggers[0].closeCalls
		f.debuggers[0].mu.Unlock()
		if closes != 1 {
			f.t.Fatalf("Plan reclamation did not close the unknown hosted debugger: closes=%d", closes)
		}
	default:
		f.t.Fatalf("narrow-parent assertion cannot inspect %s", f.operation.name)
	}
}
