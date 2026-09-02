package core

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/google/uuid"
)

func TestConnectionContainsOnlyLifetimeState(t *testing.T) {
	typeOfConnection := reflect.TypeFor[Connection]()
	forbiddenFields := map[string]struct{}{
		"plans":         {},
		"executions":    {},
		"debugSessions": {},
		"runtime":       {},
	}
	for index := range typeOfConnection.NumField() {
		field := typeOfConnection.Field(index)
		if _, forbidden := forbiddenFields[field.Name]; forbidden {
			t.Fatalf("Connection retained forbidden field %q", field.Name)
		}
	}

	methods := reflect.TypeFor[*Connection]()
	for _, name := range []string{
		"Compile",
		"Execute",
		"OpenDebugSession",
		"ReleasePlan",
		"ReleaseExecution",
		"ReleaseDebugSession",
	} {
		if _, exists := methods.MethodByName(name); exists {
			t.Fatalf("Connection retained forbidden method %q", name)
		}
	}
}

func TestContextCombinesRequestAndConnectionCancellation(t *testing.T) {
	connection := NewConnection()
	request, cancelRequest := context.WithCancel(context.Background())
	operation, cancelOperation := NewContext(request, connection)
	t.Cleanup(cancelOperation)

	if operation.Connection() != connection {
		t.Fatal("operation context did not retain its logical connection")
	}

	cancelRequest()
	select {
	case <-operation.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("request cancellation did not cancel the operation context")
	}

	connection = NewConnection()
	operation, cancelOperation = NewContext(context.Background(), connection)
	t.Cleanup(cancelOperation)
	if !connection.beginClose() {
		t.Fatal("connection close did not begin")
	}

	select {
	case <-operation.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection cancellation did not cancel the operation context")
	}
	connection.finishClose(nil)
}

func TestGlobalRegistriesEnforceOwnershipAndParentIndexes(t *testing.T) {
	plan := &spyPlan{
		newSession: func(context.Context, sessionOptions) (api.Session, error) {
			return &spySession{}, nil
		},
		newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
			return &spyDebugger{}, nil
		},
	}
	host, err := newTestHost(&spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	owner, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}
	other, err := host.OpenConnection()
	if err != nil {
		t.Fatal(err)
	}

	compiled, err := owner.Compile(context.Background(), CompileInput{
		Source:     api.Source{Content: "RETURN 1"},
		Debuggable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := owner.Execute(context.Background(), ExecuteInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := owner.OpenDebugSession(context.Background(), OpenDebugInput{PlanID: compiled.ID})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := host.plans.get(other.ID(), compiled.ID); !hasCategory(err, ErrorPlanNotFound) {
		t.Fatalf("cross-owner plan lookup was not hidden: %v", err)
	}
	if _, err := host.executions.get(other.ID(), execution.ID); !hasCategory(err, ErrorExecutionNotFound) {
		t.Fatalf("cross-owner execution lookup was not hidden: %v", err)
	}
	if _, err := host.sessions.get(other.ID(), opened.ID); !hasCategory(err, ErrorDebugSessionNotFound) {
		t.Fatalf("cross-owner debug lookup was not hidden: %v", err)
	}

	if ids := host.plans.listByOwner(owner.ID()); !reflect.DeepEqual(ids, []PlanID{compiled.ID}) {
		t.Fatalf("unexpected owner plan index: %#v", ids)
	}
	if ids := host.executions.listByPlan(owner.ID(), compiled.ID); !reflect.DeepEqual(ids, []ExecutionID{execution.ID}) {
		t.Fatalf("unexpected plan execution index: %#v", ids)
	}
	if ids := host.sessions.listByPlan(owner.ID(), compiled.ID); !reflect.DeepEqual(ids, []DebugSessionID{opened.ID}) {
		t.Fatalf("unexpected plan debug index: %#v", ids)
	}

	if err := owner.ReleasePlan(testContext(t), compiled.ID); err != nil {
		t.Fatal(err)
	}
	if ids := host.plans.listByOwner(owner.ID()); len(ids) != 0 {
		t.Fatalf("released plan remained indexed: %#v", ids)
	}
	if ids := host.executions.listByPlan(owner.ID(), compiled.ID); len(ids) != 0 {
		t.Fatalf("released executions remained indexed: %#v", ids)
	}
	if ids := host.sessions.listByPlan(owner.ID(), compiled.ID); len(ids) != 0 {
		t.Fatalf("released debug sessions remained indexed: %#v", ids)
	}
}

func TestGlobalRegistriesRetainCapacityThroughClosing(t *testing.T) {
	owner := NewConnection().ID()
	other := NewConnection().ID()
	planID := PlanID(uuid.NewString())

	t.Run("connections", func(t *testing.T) {
		registry := NewConnectionRegistry(1)
		connection := NewConnection()
		if err := registry.Register(connection); err != nil {
			t.Fatal(err)
		}

		closing, started, err := registry.beginClose(connection.ID())
		if err != nil || !started || closing != connection {
			t.Fatalf("unexpected close transition: connection=%p started=%v err=%v", closing, started, err)
		}
		if err := registry.Register(NewConnection()); !hasCategory(err, ErrorResourceExhausted) {
			t.Fatalf("closing connection did not retain capacity: %v", err)
		}

		registry.remove(connection.ID(), connection)
		connection.finishClose(nil)
		if err := registry.Register(NewConnection()); err != nil {
			t.Fatalf("settled close did not release capacity: %v", err)
		}
	})

	t.Run("plans", func(t *testing.T) {
		registry := NewPlanRegistry(1)
		if err := registry.reserve(owner); err != nil {
			t.Fatal(err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorResourceExhausted) {
			t.Fatalf("pending plan did not retain capacity: %v", err)
		}
		registry.rollback(owner)

		if err := registry.reserve(owner); err != nil {
			t.Fatal(err)
		}
		plan := &Plan{id: planID, owner: owner}
		if err := registry.commit(plan); err != nil {
			t.Fatal(err)
		}
		if _, err := registry.get(other, planID); !hasCategory(err, ErrorPlanNotFound) {
			t.Fatalf("cross-owner plan lookup was not hidden: %v", err)
		}

		closing, started, err := registry.beginClose(owner, planID)
		if err != nil || !started || closing != plan {
			t.Fatalf("unexpected close transition: plan=%p started=%v err=%v", closing, started, err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorResourceExhausted) {
			t.Fatalf("closing plan did not retain capacity: %v", err)
		}

		registry.remove(plan)
		plan.finishClose(nil)
		if err := registry.reserve(owner); err != nil {
			t.Fatalf("settled plan close did not release capacity: %v", err)
		}
		registry.rollback(owner)
	})

	t.Run("executions", func(t *testing.T) {
		registry := NewExecutionRegistry(1, 1)
		if err := registry.reserve(owner); err != nil {
			t.Fatal(err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorResourceExhausted) {
			t.Fatalf("pending execution did not retain capacity: %v", err)
		}
		registry.rollback(owner)

		if err := registry.reserve(owner); err != nil {
			t.Fatal(err)
		}
		execution := &Execution{id: ExecutionID(uuid.NewString()), owner: owner, planID: planID}
		if err := registry.commit(execution); err != nil {
			t.Fatal(err)
		}
		if ids := registry.listByPlan(owner, planID); !reflect.DeepEqual(ids, []ExecutionID{execution.id}) {
			t.Fatalf("unexpected plan execution index: %#v", ids)
		}
		if _, err := registry.get(other, execution.id); !hasCategory(err, ErrorExecutionNotFound) {
			t.Fatalf("cross-owner execution lookup was not hidden: %v", err)
		}

		closing, started, err := registry.beginClose(owner, execution.id)
		if err != nil || !started || closing != execution {
			t.Fatalf("unexpected close transition: execution=%p started=%v err=%v", closing, started, err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorResourceExhausted) {
			t.Fatalf("closing execution did not retain capacity: %v", err)
		}

		registry.remove(execution)
		execution.release.Finish(nil)
		if err := registry.reserve(owner); err != nil {
			t.Fatalf("settled execution close did not release capacity: %v", err)
		}
		registry.rollback(owner)
	})

	t.Run("debug sessions", func(t *testing.T) {
		registry := NewDebugSessionRegistry(1, 1, 1)
		if err := registry.reserve(owner); err != nil {
			t.Fatal(err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorResourceExhausted) {
			t.Fatalf("pending debug session did not retain capacity: %v", err)
		}
		registry.rollback(owner)

		if err := registry.reserve(owner); err != nil {
			t.Fatal(err)
		}
		session := &DebugSession{id: DebugSessionID(uuid.NewString()), owner: owner, planID: planID}
		if err := registry.commit(session); err != nil {
			t.Fatal(err)
		}
		if ids := registry.listByPlan(owner, planID); !reflect.DeepEqual(ids, []DebugSessionID{session.id}) {
			t.Fatalf("unexpected plan debug index: %#v", ids)
		}
		if _, err := registry.get(other, session.id); !hasCategory(err, ErrorDebugSessionNotFound) {
			t.Fatalf("cross-owner debug lookup was not hidden: %v", err)
		}

		closing, started, err := registry.beginClose(owner, session.id)
		if err != nil || !started || closing != session {
			t.Fatalf("unexpected close transition: session=%p started=%v err=%v", closing, started, err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorResourceExhausted) {
			t.Fatalf("closing debug session did not retain capacity: %v", err)
		}

		registry.remove(session)
		session.release.Finish(nil)
		if err := registry.reserve(owner); err != nil {
			t.Fatalf("settled debug close did not release capacity: %v", err)
		}
		registry.rollback(owner)
	})
}

func TestConnectionRegistryRejectsRegistrationAfterShutdownBegins(t *testing.T) {
	registry := NewConnectionRegistry(2)
	connection := NewConnection()
	if err := registry.Register(connection); err != nil {
		t.Fatal(err)
	}

	ids := registry.beginShutdown()
	if !reflect.DeepEqual(ids, []ConnectionID{connection.ID()}) {
		t.Fatalf("unexpected shutdown snapshot: %#v", ids)
	}
	if err := registry.Register(NewConnection()); !hasCategory(err, ErrorInvalidState) {
		t.Fatalf("registration succeeded during shutdown: %v", err)
	}
}
