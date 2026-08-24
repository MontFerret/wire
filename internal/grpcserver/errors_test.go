package grpcserver

import (
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCErrorMapsResourceExhaustionAndBreakpointMetadata(t *testing.T) {
	tests := []struct {
		name       string
		domain     *core.DomainError
		code       codes.Code
		category   wirev1.ErrorCategory
		resource   wirev1.ResourceKind
		resourceID string
	}{
		{
			name:     "resource exhaustion",
			domain:   &core.DomainError{Category: core.ErrorResourceExhausted, Message: "plan limit reached"},
			code:     codes.ResourceExhausted,
			category: wirev1.ErrorCategory_ERROR_CATEGORY_RESOURCE_EXHAUSTED,
		},
		{
			name: "breakpoint not found",
			domain: &core.DomainError{
				Category:   core.ErrorBreakpointNotFound,
				Message:    "resource not found",
				ResourceID: "42",
			},
			code:       codes.NotFound,
			category:   wirev1.ErrorCategory_ERROR_CATEGORY_BREAKPOINT_NOT_FOUND,
			resource:   wirev1.ResourceKind_RESOURCE_KIND_BREAKPOINT,
			resourceID: "42",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, ok := status.FromError(rpcError(test.domain))
			if !ok || converted.Code() != test.code {
				t.Fatalf("unexpected gRPC status: %v", converted)
			}

			var detail *wirev1.ErrorDetail
			for _, value := range converted.Details() {
				if typed, detailOK := value.(*wirev1.ErrorDetail); detailOK {
					detail = typed
					break
				}
			}
			if detail == nil || detail.GetCategory() != test.category || detail.GetResource() != test.resource || detail.GetResourceId() != test.resourceID {
				t.Fatalf("unexpected Wire error detail: %#v", detail)
			}
		})
	}
}
