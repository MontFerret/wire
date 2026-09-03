package client

import (
	"errors"
	"reflect"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDecodeErrorPreservesDomainDetailsAndTransportStatus(t *testing.T) {
	detail := &wirev1.ErrorDetail{
		Category: wirev1.ErrorCategory_ERROR_CATEGORY_COMPILATION_FAILURE,
	}
	withDetails, err := status.New(codes.InvalidArgument, "compilation failed").WithDetails(detail)
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

	if len(wireErr.Diagnostics) != 0 {
		t.Fatalf("Wire fabricated diagnostics: %#v", wireErr.Diagnostics)
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
