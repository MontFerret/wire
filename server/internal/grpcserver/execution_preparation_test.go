package grpcserver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/diagnostics"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestExecutionPreparationPreservesDiagnostics(t *testing.T) {
	expected := diagnostics.Diagnostics{{Kind: "SyntaxError", Message: "expected expression", Source: api.NewSource("query.fql", "RETURN )")}}
	service, _, request := newOutputExecution(t, func(context.Context) (api.Output, error) {
		return nil, errors.Join(expected, errors.New("private host cause"))
	})

	err := service.ReadExecutionOutput(request, &executionOutputStream{ctx: t.Context(), send: func(*wirev1.ReadExecutionOutputResponse) error {
		t.Error("preparation failure emitted an output header")

		return nil
	}})
	if err == nil || strings.Contains(err.Error(), "private host cause") {
		t.Fatalf("unsanitized preparation error: %v", err)
	}

	var category wirev1.ErrorCategory
	var values *wirev1.DiagnosticSet
	for _, detail := range status.Convert(err).Details() {
		switch value := detail.(type) {
		case *wirev1.ErrorDetail:
			category = value.GetCategory()
		case *wirev1.DiagnosticSet:
			values = value
		}
	}

	if category != wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE || values == nil || len(values.GetDiagnostics()) != 1 {
		t.Fatalf("lost query classification or diagnostics: category=%v details=%v error=%v", category, values, err)
	}
}
