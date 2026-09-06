package grpcserver

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestUnaryRecoverySanitizesPanicValues(t *testing.T) {
	_, err := UnaryRecoveryInterceptor(context.Background(), nil, nil, func(context.Context, any) (any, error) {
		panic("secret panic value")
	})
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "secret") {
		t.Fatalf("panic was not sanitized: %v", err)
	}

	grpcStatus := status.Convert(err)
	if len(grpcStatus.Details()) != 1 {
		t.Fatalf("missing structured error detail: %v", err)
	}

	detail, ok := grpcStatus.Details()[0].(*wirev1.ErrorDetail)
	if !ok || detail.GetCategory() != wirev1.ErrorCategory_ERROR_CATEGORY_INTERNAL_RUNTIME_FAILURE {
		t.Fatalf("unexpected error detail: %#v", grpcStatus.Details())
	}
}
