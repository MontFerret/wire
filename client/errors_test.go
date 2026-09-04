package client

import (
	"errors"
	"reflect"
	"testing"

	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDecodeErrorPreservesDomainDetailsAndTransportStatus(t *testing.T) {
	detail := &wirev1.ErrorDetail{
		Category: wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE,
	}
	diagnosticSet := &wirev1.DiagnosticSet{Diagnostics: []*wirev1.Diagnostic{{
		Kind:    "SyntaxError",
		Message: "expected an expression",
		Hint:    "provide a value",
		Note:    "the expression is required",
		Source:  &wirev1.Source{Name: "query.fql", Content: "RETURN"},
		Annotations: []*wirev1.DiagnosticAnnotation{{
			Range: &wirev1.Range{
				Location: &wirev1.Location{SourceName: "query.fql", Position: &wirev1.Position{Line: 1, Column: 6}},
				Span:     &wirev1.Span{Start: 6, End: 6},
			},
			Message: "missing here",
			Primary: true,
		}},
	}}}
	withDetails, err := status.New(codes.InvalidArgument, "compilation failed").WithDetails(detail, diagnosticSet)
	if err != nil {
		t.Fatal(err)
	}
	transportErr := withDetails.Err()
	decoded := decodeError(transportErr)

	var wireErr *Error
	if !errors.As(decoded, &wireErr) {
		t.Fatalf("decoded error lost its structured type: %#v", decoded)
	}

	if wireErr.Category != ErrorCompilation || wireErr.Message != "compilation failed" || status.Code(decoded) != codes.InvalidArgument {
		t.Fatalf("unexpected structured error: %#v", wireErr)
	}

	if !errors.Is(decoded, transportErr) {
		t.Fatal("decoded error did not unwrap to its transport cause")
	}

	wantDiagnostics := diagnostics.Diagnostics{{
		Kind:    diagnostics.Kind("SyntaxError"),
		Message: "expected an expression",
		Hint:    "provide a value",
		Note:    "the expression is required",
		Source:  source.New("query.fql", "RETURN"),
		Annotations: []diagnostics.Annotation{{
			Range: source.Range{
				Location: source.Location{Position: source.Position{Line: 1, Column: 6}, SourceName: "query.fql"},
				Span:     source.Span{Start: 6, End: 6},
			},
			Message: "missing here",
			Primary: true,
		}},
	}}
	if !reflect.DeepEqual(wireErr.Diagnostics, wantDiagnostics) {
		t.Fatalf("Wire changed diagnostics: %#v", wireErr.Diagnostics)
	}

	joined := errors.Join(ErrClosed, decoded)
	if !errors.Is(joined, ErrClosed) || !errors.As(joined, &wireErr) {
		t.Fatalf("joined error lost its components: %v", joined)
	}
}

func TestErrorPublicModelOmitsTransportCodeAndResourceIdentifiers(t *testing.T) {
	errorType := reflect.TypeFor[Error]()
	for _, name := range []string{"Code", "ResourceID"} {
		if _, ok := errorType.FieldByName(name); ok {
			t.Errorf("Error still exports %s", name)
		}
	}
}
