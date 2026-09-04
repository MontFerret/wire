package failure_test

import (
	"errors"
	"testing"

	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestFailureImplementsErrorAndPreservesCanonicalDiagnostics(t *testing.T) {
	diagnosticSet := diagnostics.Diagnostics{{Kind: diagnostics.TypeError, Message: "invalid expression"}}
	terminalFailure := &failure.Failure{
		Category:    failure.CategoryCompilation,
		Message:     "compilation failed",
		Diagnostics: diagnosticSet,
	}

	var target *failure.Failure
	if !errors.As(terminalFailure, &target) || target.Error() != "compilation failed" ||
		target.Category != failure.CategoryCompilation || len(target.Diagnostics) != 1 {
		t.Fatalf("unexpected shared failure: %#v", target)
	}

	var nilFailure *failure.Failure
	if nilFailure.Error() != "" {
		t.Fatalf("nil failure returned a message: %q", nilFailure.Error())
	}
}

func TestFailureCategoriesAreDistinctAndNonZero(t *testing.T) {
	categories := []failure.Category{
		failure.CategoryCompilation,
		failure.CategoryExecution,
		failure.CategoryPlanNotFound,
		failure.CategoryExecutionNotFound,
		failure.CategoryDebugSessionNotFound,
		failure.CategoryConnectionNotFound,
		failure.CategoryInvalidState,
		failure.CategoryInternalRuntime,
		failure.CategoryWatcherLagged,
		failure.CategoryBreakpointNotFound,
	}

	seen := make(map[failure.Category]struct{}, len(categories))
	for _, category := range categories {
		if category == 0 {
			t.Fatal("a transmitted failure category used the zero value")
		}
		if _, exists := seen[category]; exists {
			t.Fatalf("duplicate failure category value %d", category)
		}

		seen[category] = struct{}{}
	}
}
