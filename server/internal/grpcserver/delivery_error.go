package grpcserver

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/wire/server/internal/core"
	"github.com/MontFerret/wire/server/internal/panicboundary"
)

// deliveryError marks a Wire limit failure crossing an execution callback. A
// hosted gRPC status alone never establishes this trusted transport provenance.
type deliveryError struct{ error }

func (e *deliveryError) GRPCStatus() *status.Status { return status.Convert(e.error) }

func outboundLimitError(message string) error {
	return &deliveryError{error: status.Error(codes.ResourceExhausted, message)}
}

func deliveryRPCError(err error) error {
	var delivery *deliveryError
	if errors.As(err, &delivery) {
		return delivery.error
	}

	// Before a metadata header, preparation errors travel as RPC statuses rather
	// than terminal output frames. Preserve canonical query diagnostics here too.
	var domain *core.DomainError
	if !errors.As(err, &domain) && len(core.DiagnosticsFromError(err)) > 0 {
		kind := core.ErrorKindExecution

		var boundary *panicboundary.Error
		if errors.As(err, &boundary) {
			kind = core.ErrorKindInternal
		}

		err = &core.DomainError{Kind: kind, Message: "runtime preparation failed", Cause: err}
	}

	return rpcError(err)
}
