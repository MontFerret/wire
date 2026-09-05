package grpcserver

import (
	"errors"
	"testing"

	"github.com/MontFerret/api"
	apidebugger "github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	wirefailure "github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/core"
)

func TestExecutionStateMapsEverySharedValue(t *testing.T) {
	tests := []struct {
		shared wireexecution.State
		want   wirev1.ExecutionState
	}{
		{shared: wireexecution.StateRunning, want: wirev1.ExecutionState_EXECUTION_STATE_RUNNING},
		{shared: wireexecution.StateCompleted, want: wirev1.ExecutionState_EXECUTION_STATE_COMPLETED},
		{shared: wireexecution.StateFailed, want: wirev1.ExecutionState_EXECUTION_STATE_FAILED},
		{shared: wireexecution.StateCancelled, want: wirev1.ExecutionState_EXECUTION_STATE_CANCELLED},
	}

	for _, test := range tests {
		got, err := executionState(test.shared)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("executionState(%v) = %v, want %v", test.shared, got, test.want)
		}
	}

	assertInternalConversionError(t, executionStateError(0))
}

func TestDebugStateAndEventKindMapEverySharedValue(t *testing.T) {
	states := []struct {
		shared wiredebugger.State
		want   wirev1.DebugState
	}{
		{shared: wiredebugger.StateCreated, want: wirev1.DebugState_DEBUG_STATE_CREATED},
		{shared: wiredebugger.StateRunning, want: wirev1.DebugState_DEBUG_STATE_RUNNING},
		{shared: wiredebugger.StateStopped, want: wirev1.DebugState_DEBUG_STATE_STOPPED},
		{shared: wiredebugger.StateCompleted, want: wirev1.DebugState_DEBUG_STATE_COMPLETED},
		{shared: wiredebugger.StateFailed, want: wirev1.DebugState_DEBUG_STATE_FAILED},
		{shared: wiredebugger.StateTerminated, want: wirev1.DebugState_DEBUG_STATE_TERMINATED},
	}
	for _, test := range states {
		got, err := debugState(test.shared)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("debugState(%v) = %v, want %v", test.shared, got, test.want)
		}
	}

	kinds := []struct {
		shared wiredebugger.EventKind
		want   wirev1.DebugEventKind
	}{
		{shared: wiredebugger.EventStarted, want: wirev1.DebugEventKind_DEBUG_EVENT_KIND_STARTED},
		{shared: wiredebugger.EventContinued, want: wirev1.DebugEventKind_DEBUG_EVENT_KIND_CONTINUED},
		{shared: wiredebugger.EventStopped, want: wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED},
		{shared: wiredebugger.EventCompleted, want: wirev1.DebugEventKind_DEBUG_EVENT_KIND_COMPLETED},
		{shared: wiredebugger.EventFailed, want: wirev1.DebugEventKind_DEBUG_EVENT_KIND_FAILED},
		{shared: wiredebugger.EventTerminated, want: wirev1.DebugEventKind_DEBUG_EVENT_KIND_TERMINATED},
		{shared: wiredebugger.EventCreated, want: wirev1.DebugEventKind_DEBUG_EVENT_KIND_CREATED},
	}
	for _, test := range kinds {
		got, err := debugEventKind(test.shared)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("debugEventKind(%v) = %v, want %v", test.shared, got, test.want)
		}
	}

	_, stateErr := debugState(0)
	assertInternalConversionError(t, stateErr)
	_, kindErr := debugEventKind(0)
	assertInternalConversionError(t, kindErr)
}

func TestFailureCategoryMapsEverySharedValue(t *testing.T) {
	tests := []struct {
		shared wirefailure.Category
		want   wirev1.ErrorCategory
	}{
		{shared: wirefailure.CategoryCompilation, want: wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE},
		{shared: wirefailure.CategoryExecution, want: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE},
		{shared: wirefailure.CategoryPlanNotFound, want: wirev1.ErrorCategory_ERROR_CATEGORY_PLAN_NOT_FOUND},
		{shared: wirefailure.CategoryExecutionNotFound, want: wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_NOT_FOUND},
		{shared: wirefailure.CategoryDebugSessionNotFound, want: wirev1.ErrorCategory_ERROR_CATEGORY_DEBUG_SESSION_NOT_FOUND},
		{shared: wirefailure.CategoryConnectionNotFound, want: wirev1.ErrorCategory_ERROR_CATEGORY_CONNECTION_NOT_FOUND},
		{shared: wirefailure.CategoryInvalidState, want: wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE},
		{shared: wirefailure.CategoryInternalRuntime, want: wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE},
		{shared: wirefailure.CategoryWatcherLagged, want: wirev1.ErrorCategory_ERROR_CATEGORY_WATCHER_LAGGED},
		{shared: wirefailure.CategoryBreakpointNotFound, want: wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND},
		{shared: wirefailure.CategorySessionNotFound, want: wirev1.ErrorCategory_ERROR_CATEGORY_SESSION_NOT_FOUND},
	}

	for _, test := range tests {
		got, err := failureCategory(test.shared)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Errorf("failureCategory(%v) = %v, want %v", test.shared, got, test.want)
		}
	}

	_, err := failureCategory(0)
	assertInternalConversionError(t, err)
}

func TestSharedEventsPreserveFieldsAndDetachMutableData(t *testing.T) {
	location := &source.Range{
		Location: source.Location{SourceName: "query.fql", Position: source.Position{Line: 2, Column: 1}},
		Span:     source.Span{Start: 3, End: 8},
	}
	content := []byte("partial")
	hitIDs := []apidebugger.BreakpointID{7}
	diagnosticSet := diagnostics.Diagnostics{{
		Kind: diagnostics.TypeError,
		Annotations: []diagnostics.Annotation{{
			Range: source.Range{Location: source.Location{SourceName: "query.fql", Position: source.Position{Line: 2}}},
		}},
	}}
	terminalFailure := &wirefailure.Failure{
		Category:    wirefailure.CategoryExecution,
		Message:     "runtime operation failed",
		Diagnostics: diagnosticSet,
	}

	converted, err := debugEvent("debug-1", wiredebugger.Event{
		Sequence: 11,
		Kind:     wiredebugger.EventStopped,
		Snapshot: wiredebugger.Snapshot{
			State:            wiredebugger.StateStopped,
			StopReason:       apidebugger.ReasonBreakpoint,
			Location:         location,
			HitBreakpointIDs: hitIDs,
			Depth:            4,
			Output:           &api.Output{ContentType: "text/plain", Content: content},
			Failure:          terminalFailure,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	content[0] = 'X'
	hitIDs[0] = 99
	location.Location.SourceName = "changed.fql"
	diagnosticSet[0].Annotations[0].Message = "changed"
	if converted.GetSequence() != 11 || converted.GetKind() != wirev1.DebugEventKind_DEBUG_EVENT_KIND_STOPPED ||
		converted.GetSession().GetStopReason() != wirev1.DebugStopReason_DEBUG_STOP_REASON_BREAKPOINT ||
		converted.GetSession().GetLocation().GetLocation().GetSourceName() != "query.fql" ||
		converted.GetSession().GetHitBreakpointIds()[0] != 7 || converted.GetSession().GetDepth() != 4 ||
		string(converted.GetSession().GetOutput().GetContent()) != "partial" ||
		converted.GetSession().GetFailure().GetDiagnosticSet().GetDiagnostics()[0].GetAnnotations()[0].GetMessage() != "" {
		t.Fatalf("debug event conversion changed or retained mutable values: %#v", converted)
	}
}

func executionStateError(state wireexecution.State) error {
	_, err := executionState(state)

	return err
}

func assertInternalConversionError(t *testing.T, err error) {
	t.Helper()

	var domain *core.DomainError
	if !errors.As(err, &domain) || domain.Kind != core.ErrorKindInternal {
		t.Fatalf("unexpected conversion error: %v", err)
	}
}
