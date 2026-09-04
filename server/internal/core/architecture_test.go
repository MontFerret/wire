package core

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	wireruntime "github.com/MontFerret/wire/pkg/runtime"
	"github.com/MontFerret/wire/server/internal/panicboundary"
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

func TestWireOwnedCommitPanicPropagatesWithoutBoundary(t *testing.T) {
	registry := NewPlanRegistry(1)
	plan := &Plan{id: "plan", owner: "owner"}
	if err := registry.reserve(plan.owner); err != nil {
		t.Fatal(err)
	}

	if err := registry.commit(plan); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("Wire defect")
	recovered := func() (value any) {
		defer func() { value = recover() }()

		_ = registry.commitChild(plan.owner, plan.id, plan, func() error {
			panic(sentinel)
		})

		return nil
	}()
	if recovered != sentinel {
		t.Fatalf("Wire-owned panic was changed: %#v", recovered)
	}

	if _, contained := recovered.(*panicboundary.Error); contained {
		t.Fatalf("Wire-owned panic was contained by panicboundary: %#v", recovered)
	}
}

func TestOperationAggregatesDelegateSupportingInfrastructure(t *testing.T) {
	t.Run("execution", func(t *testing.T) {
		typeOfExecution := reflect.TypeFor[Execution]()
		for _, name := range []string{
			"session",
			"maxWatchers",
			"sequence",
			"lastEvent",
			"nextWatcher",
			"subscriptions",
			"watchers",
		} {
			if _, exists := typeOfExecution.FieldByName(name); exists {
				t.Fatalf("Execution retained delegated field %q", name)
			}
		}

		events, exists := typeOfExecution.FieldByName("events")
		if !exists || events.Type != reflect.TypeFor[*eventStream[wireruntime.Event]]() {
			t.Fatalf("Execution does not own the shared event stream: %v", events.Type)
		}
	})

	t.Run("debug session", func(t *testing.T) {
		typeOfSession := reflect.TypeFor[DebugSession]()
		for _, name := range []string{
			"debugger",
			"reason",
			"location",
			"hitIDs",
			"depth",
			"output",
			"failure",
			"maxWatchers",
			"maxBreakpoints",
			"sequence",
			"lastEvent",
			"nextWatcher",
			"subscriptions",
			"watchers",
		} {
			if _, exists := typeOfSession.FieldByName(name); exists {
				t.Fatalf("DebugSession retained delegated field %q", name)
			}
		}

		state, exists := typeOfSession.FieldByName("state")
		if !exists || state.Type != reflect.TypeFor[debugSessionState]() {
			t.Fatalf("DebugSession does not own cohesive debug state: %v", state.Type)
		}

		breakpoints, exists := typeOfSession.FieldByName("breakpoints")
		if !exists || breakpoints.Type != reflect.TypeFor[*breakpointSet]() {
			t.Fatalf("DebugSession does not own the breakpoint component: %v", breakpoints.Type)
		}

		events, exists := typeOfSession.FieldByName("events")
		if !exists || events.Type != reflect.TypeFor[*eventStream[wiredebugger.Event]]() {
			t.Fatalf("DebugSession does not own the shared event stream: %v", events.Type)
		}

		controller, exists := typeOfSession.FieldByName("controller")
		if !exists || controller.Type != reflect.TypeFor[*DebugController]() {
			t.Fatalf("DebugSession does not own the debug controller: %v", controller.Type)
		}

		typeOfBreakpoints := reflect.TypeFor[breakpointSet]()
		for _, name := range []string{"mu", "session"} {
			if _, exists := typeOfBreakpoints.FieldByName(name); exists {
				t.Fatalf("breakpointSet retained delegated field %q", name)
			}
		}
	})
}

func TestPrincipalReceiversAndDebuggerHandleStayWithTheirOwners(t *testing.T) {
	packages, err := parser.ParseDir(token.NewFileSet(), ".", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	parsed := packages["core"]
	for filename, file := range parsed.Files {
		base := filepath.Base(filename)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}

		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && function.Recv != nil && len(function.Recv.List) == 1 {
				receiver := receiverTypeName(function.Recv.List[0].Type)
				switch receiver {
				case "DebugSession":
					if base != "debug_session.go" {
						t.Errorf("DebugSession method %s is in %s", function.Name.Name, base)
					}
				case "DebugController":
					if base != "debug_controller.go" {
						t.Errorf("DebugController method %s is in %s", function.Name.Name, base)
					}
				case "Execution":
					if base != "execution.go" {
						t.Errorf("Execution method %s is in %s", function.Name.Name, base)
					}
				case "breakpointSet":
					if base != "breakpoint_set.go" {
						t.Errorf("breakpointSet method %s is in %s", function.Name.Name, base)
					}
				}
			}

			generic, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}

			for _, specification := range generic.Specs {
				typeSpec, ok := specification.(*ast.TypeSpec)
				if !ok {
					continue
				}

				structure, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}

				for _, field := range structure.Fields.List {
					if debuggerSessionType(field.Type) && typeSpec.Name.Name != "DebugController" {
						t.Errorf("%s stores debugger.Session in %s", typeSpec.Name.Name, base)
					}
				}
			}
		}
	}
}

func receiverTypeName(expression ast.Expr) string {
	if pointer, ok := expression.(*ast.StarExpr); ok {
		expression = pointer.X
	}

	identifier, _ := expression.(*ast.Ident)
	if identifier == nil {
		return ""
	}

	return identifier.Name
}

func debuggerSessionType(expression ast.Expr) bool {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Session" {
		return false
	}

	packageName, _ := selector.X.(*ast.Ident)

	return packageName != nil && packageName.Name == "debugger"
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

	if _, err := host.plans.get(other.ID(), compiled.ID); !hasCategory(err, ErrorKindPlanNotFound) {
		t.Fatalf("cross-owner plan lookup was not hidden: %v", err)
	}
	if _, err := host.executions.get(other.ID(), execution.ID); !hasCategory(err, ErrorKindExecutionNotFound) {
		t.Fatalf("cross-owner execution lookup was not hidden: %v", err)
	}
	if _, err := host.sessions.get(other.ID(), opened.ID); !hasCategory(err, ErrorKindDebugSessionNotFound) {
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
		if err := registry.Register(NewConnection()); !hasCategory(err, ErrorKindResourceExhausted) {
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
		if err := registry.reserve(owner); !hasCategory(err, ErrorKindResourceExhausted) {
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
		if _, err := registry.get(other, planID); !hasCategory(err, ErrorKindPlanNotFound) {
			t.Fatalf("cross-owner plan lookup was not hidden: %v", err)
		}

		closing, started, err := registry.beginClose(owner, planID)
		if err != nil || !started || closing != plan {
			t.Fatalf("unexpected close transition: plan=%p started=%v err=%v", closing, started, err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorKindResourceExhausted) {
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
		if err := registry.reserve(owner); !hasCategory(err, ErrorKindResourceExhausted) {
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
		if _, err := registry.get(other, execution.id); !hasCategory(err, ErrorKindExecutionNotFound) {
			t.Fatalf("cross-owner execution lookup was not hidden: %v", err)
		}

		closing, started, err := registry.beginClose(owner, execution.id)
		if err != nil || !started || closing != execution {
			t.Fatalf("unexpected close transition: execution=%p started=%v err=%v", closing, started, err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorKindResourceExhausted) {
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
		if err := registry.reserve(owner); !hasCategory(err, ErrorKindResourceExhausted) {
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
		if _, err := registry.get(other, session.id); !hasCategory(err, ErrorKindDebugSessionNotFound) {
			t.Fatalf("cross-owner debug lookup was not hidden: %v", err)
		}

		closing, started, err := registry.beginClose(owner, session.id)
		if err != nil || !started || closing != session {
			t.Fatalf("unexpected close transition: session=%p started=%v err=%v", closing, started, err)
		}
		if err := registry.reserve(owner); !hasCategory(err, ErrorKindResourceExhausted) {
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
	if err := registry.Register(NewConnection()); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("registration succeeded during shutdown: %v", err)
	}
}
