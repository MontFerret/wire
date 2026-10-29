package grpcserver

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	return rpcError(err)
}
