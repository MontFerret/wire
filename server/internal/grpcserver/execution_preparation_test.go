package grpcserver

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/diagnostics"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	wirefailure "github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/server/internal/core"
	"github.com/MontFerret/wire/server/internal/panicboundary"
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

	assertPreparationStatus(t, err, codes.Internal, wirev1.ErrorCategory_ERROR_CATEGORY_EXECUTION_FAILURE, expected)
}

func TestExecutionPreparationCleanupPanicPreservesDiagnostics(t *testing.T) {
	const secret = "private output cleanup panic"
	expected := diagnostics.Diagnostics{{Kind: "SyntaxError", Message: "expected expression", Hint: "supply a value", Source: api.NewSource("query.fql", "RETURN )")}}
	hosted := &executionOutputSpy{cleanup: func() error { panic(secret) }}
	service, execution, request := newOutputExecution(t, func(context.Context) (api.Output, error) {
		return hosted, expected
	})

	err := service.ReadExecutionOutput(request, &executionOutputStream{ctx: testDeliveryContext(t), send: func(*wirev1.ReadExecutionOutputResponse) error {
		t.Error("preparation failure emitted a frame")

		return nil
	}})
	snapshot := execution.Snapshot()
	releaseErr := execution.Release(testDeliveryContext(t))

	var boundary *panicboundary.Error
	if !errors.As(releaseErr, &boundary) || boundary.Value != secret || hosted.closes.Load() != 1 {
		t.Fatalf("release error=%v closes=%d", releaseErr, hosted.closes.Load())
	}

	if snapshot.State != wireexecution.StateFailed || snapshot.Failure == nil || snapshot.Failure.Category != wirefailure.CategoryInternalRuntime ||
		!reflect.DeepEqual(snapshot.Failure.Diagnostics, expected) || strings.Contains(snapshot.Failure.Message, secret) {
		t.Fatalf("unexpected preparation snapshot: %+v", snapshot)
	}

	if strings.Contains(status.Convert(err).Proto().String(), secret) {
		t.Fatal("RPC status exposed the private panic value")
	}

	assertPreparationStatus(t, err, codes.Internal, wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE, expected)
}

func TestDeliveryRPCErrorPrecedence(t *testing.T) {
	query := diagnostics.Diagnostics{{Message: "query diagnostic"}}
	mixed := errors.Join(query, &panicboundary.Error{Value: "private panic"})
	for _, test := range []struct {
		name     string
		cause    error
		code     codes.Code
		category wirev1.ErrorCategory
		details  diagnostics.Diagnostics
	}{
		{"domain", &core.DomainError{Kind: core.ErrorKindInvalidState, Message: "invalid state"}, codes.FailedPrecondition, wirev1.ErrorCategory_ERROR_CATEGORY_INVALID_STATE, query},
		{"cancellation", context.Canceled, codes.Canceled, wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED, nil},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded, wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED, nil},
		{"trusted limit", errors.Join(outboundLimitError("limit exceeded"), context.Canceled), codes.ResourceExhausted, wirev1.ErrorCategory_ERROR_CATEGORY_UNSPECIFIED, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertPreparationStatus(t, deliveryRPCError(errors.Join(mixed, test.cause)), test.code, test.category, test.details)
		})
	}
}

func assertPreparationStatus(t *testing.T, err error, code codes.Code, category wirev1.ErrorCategory, expected diagnostics.Diagnostics) {
	t.Helper()

	converted := status.Convert(err)
	if converted.Code() != code {
		t.Fatalf("got status %v, want %v", converted, code)
	}

	var actual wirev1.ErrorCategory
	var values *wirev1.DiagnosticSet
	for _, detail := range converted.Details() {
		switch value := detail.(type) {
		case *wirev1.ErrorDetail:
			actual = value.GetCategory()
		case *wirev1.DiagnosticSet:
			values = value
		default:
			t.Fatalf("unexpected status detail: %T", value)
		}
	}

	want, conversionErr := diagnosticsToProto(expected)
	if conversionErr != nil {
		t.Fatal(conversionErr)
	}

	if actual != category || !proto.Equal(values, want) {
		t.Fatalf("got category=%v diagnostics=%v, want category=%v diagnostics=%v", actual, values, category, want)
	}
}
