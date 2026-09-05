package integration_test

import (
	"context"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/test/integration/harness"
)

type (
	allocationOperation struct {
		name                           string
		method, release, parentRelease harness.Operation
	}

	runtimeAllocationFixture struct {
		t                  *testing.T
		h                  *harness.Harness
		gate               *harness.Faults
		reply              *harness.ResponseGate
		record             *harness.Recorder
		remote, other      api.Runtime
		plan               api.Plan
		session            api.Session
		operation          allocationOperation
		expectedCloseError error
	}
)

func allocationOperations() []allocationOperation {
	return []allocationOperation{
		{"compile", harness.Compile, harness.ReleasePlan, harness.CloseRuntime},
		{"compile debug", harness.CompileDebug, harness.ReleasePlan, harness.CloseRuntime},
		{"session", harness.CreateSession, harness.ReleaseSession, harness.ReleasePlan},
		{"debug session", harness.CreateDebugger, harness.ReleaseDebugger, harness.ReleasePlan},
		{"session run", harness.RunSession, harness.ReleaseExecution, harness.ReleaseSession},
		{"runtime run", harness.RunRuntime, harness.ReleaseExecution, harness.CloseRuntime},
	}
}

func newRuntimeAllocationFixture(t *testing.T, operation allocationOperation) *runtimeAllocationFixture {
	t.Helper()
	h := harness.New(t)
	f := &runtimeAllocationFixture{t: t, h: h, gate: h.Faults(), record: h.RuntimeSpy().Recorder(), remote: h.Runtime(), operation: operation}
	var err error
	f.other, err = h.OpenRuntime()
	if err != nil {
		t.Fatal(err)
	}

	switch operation.method {
	case harness.CreateSession, harness.CreateDebugger, harness.RunSession:
		f.plan, err = f.remote.CompileDebug(h.Context(), api.Source{Content: "RETURN 1"})
		if err != nil {
			t.Fatal(err)
		}
	}

	if operation.method == harness.RunSession {
		f.session, err = f.plan.NewSession(h.Context())
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
		planOptions = []api.PlanOption{func(api.PlanOptions) error {
			cancelInOption()

			return nil
		}}
		sessionOptions = []api.SessionOption{func(api.SessionOptions) error {
			cancelInOption()

			return nil
		}}
	}

	switch f.operation.method {
	case harness.Compile, harness.CompileDebug:
		compile := f.remote.Compile

		if f.operation.method == harness.CompileDebug {
			compile = f.remote.CompileDebug
		}

		plan, err := compile(ctx, api.Source{Content: "RETURN 1"}, planOptions...)
		if err != nil {
			return nil, err
		}

		return plan.Close, nil
	case harness.CreateSession:
		session, err := f.plan.NewSession(ctx, sessionOptions...)
		if err != nil {
			return nil, err
		}

		return session.Close, nil
	case harness.CreateDebugger:
		session, err := f.plan.NewDebugSession(ctx, sessionOptions...)
		if err != nil {
			return nil, err
		}

		return session.Close, nil
	case harness.RunSession:
		_, err := f.session.Run(ctx)

		return nil, err
	default:
		_, err := f.remote.Run(ctx, api.Source{Content: "RETURN 1"}, sessionOptions...)

		return nil, err
	}
}

func (f *runtimeAllocationFixture) awaitCommitted() {
	f.t.Helper()
	harness.Await(f.t, f.reply.Committed)
}

func (f *runtimeAllocationFixture) awaitResult(result <-chan error) error {
	f.t.Helper()

	return harness.Await(f.t, result)
}

func (f *runtimeAllocationFixture) assertAllClosed() {
	f.t.Helper()
	f.record.AssertClosed(f.t)
}

func (f *runtimeAllocationFixture) assertNarrowParentClosed() {
	f.t.Helper()
	snapshot := f.record.Snapshot()
	parent := snapshot.OfKind("plan")[0]

	if f.operation.method == harness.RunSession {
		parent = snapshot.OfKind("session")[0]
	}

	if got := snapshot.Count(parent.ID, "Close"); got != 1 {
		f.t.Fatalf("narrow parent %v closed %d times", parent, got)
	}

	for _, resource := range snapshot.Resources {
		if resource.Parent == parent.ID && snapshot.Count(resource.ID, "Close") != 1 {
			f.t.Fatalf("narrow parent left child %v open", resource)
		}
	}
}
