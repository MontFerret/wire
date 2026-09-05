package integration_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// connectionLossRPCError supplies public client metadata and a transport status
// without coupling contract tests to protobuf error details.
type connectionLossRPCError struct {
	cause *client.Error
	code  codes.Code
}

func (e *connectionLossRPCError) Error() string {
	return e.cause.Error()
}

func (e *connectionLossRPCError) Unwrap() error {
	return e.cause
}

func (e *connectionLossRPCError) GRPCStatus() *status.Status {
	return status.New(e.code, e.Error())
}

func TestConnectionLossErrorClassification(t *testing.T) {
	connectionGone := &connectionLossRPCError{cause: &client.Error{Category: failure.CategoryConnectionNotFound}, code: codes.NotFound}
	executionGone := &connectionLossRPCError{cause: &client.Error{Category: failure.CategoryExecutionNotFound}, code: codes.NotFound}
	unavailable := status.Error(codes.Unavailable, "transport closed")
	unexpected := errors.New("unexpected cleanup failure")

	for _, mode := range []string{"runtime", "session", "debugger"} {
		for _, shutdown := range []bool{false, true} {
			allowMissing := shutdown && mode != "debugger"

			for _, test := range []struct {
				name string
				err  error
				want bool
			}{
				{"no error", nil, true},
				{"transport unavailable", unavailable, true},
				{"transport canceled", status.Error(codes.Canceled, "transport canceled"), true},
				{"closed handle", client.ErrClosed, true},
				{"remote cancellation", client.ErrExecutionCancelled, true},
				{"connection removed", connectionGone, allowMissing},
				{"execution removed", executionGone, allowMissing},
				{"wrapped missing execution", fmt.Errorf("watch: %w", executionGone), allowMissing},
				{"uncategorized not found", status.Error(codes.NotFound, "resource not found"), false},
				{"unrelated missing resource", &connectionLossRPCError{cause: &client.Error{Category: failure.CategoryPlanNotFound}, code: codes.NotFound}, false},
				{"incorrect status", &connectionLossRPCError{cause: &client.Error{Category: failure.CategoryConnectionNotFound}, code: codes.Internal}, false},
				{"unexpected failure", unexpected, false},
				{"expected joined failures", errors.Join(unavailable, client.ErrExecutionCancelled), true},
				{"shutdown joined failures", errors.Join(connectionGone, unavailable), allowMissing},
				{"transport hides failure", errors.Join(unavailable, unexpected), false},
				{"sentinel hides failure", errors.Join(client.ErrClosed, unexpected), false},
				{"shutdown hides failure", errors.Join(connectionGone, unexpected), false},
				{"unexpected first cause", errors.Join(unexpected, unavailable), false},
				{"wrapped nested join", fmt.Errorf("run: %w", errors.Join(unavailable, errors.Join(client.ErrExecutionCancelled, unexpected))), false},
			} {
				t.Run(mode+map[bool]string{false: "/transport/", true: "/server/"}[shutdown]+test.name, func(t *testing.T) {
					if got := expectedConnectionLoss(test.err, mode, shutdown); got != test.want {
						t.Fatalf("accepted %v = %v, want %v", test.err, got, test.want)
					}
				})
			}
		}
	}
}
