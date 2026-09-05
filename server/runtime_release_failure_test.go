package server_test

import (
	"errors"
	"testing"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeCompletedExecutionReleaseFailurePreservesParents(t *testing.T) {
	for _, index := range []int{4, 5} {
		operation := allocationOperations()[index]
		for _, acknowledged := range []bool{false, true} {
			t.Run(operation.name+map[bool]string{false: "/failed delivery", true: "/lost acknowledgement"}[acknowledged], func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				releaseErr := status.Error(codes.Unavailable, "execution release unavailable")
				fail := f.gate.fail
				if acknowledged {
					fail = f.gate.failResponse
				}

				fail(operation.release, releaseErr)
				if _, err := f.allocate(testContext(t), nil); !errors.Is(err, releaseErr) {
					t.Fatalf("successful execution lost its release failure: %v", err)
				}

				if f.gate.count(operation.release) != 1 || f.gate.count(operation.parentRelease) != 0 ||
					f.gate.count(wirev1.PlanService_ReleasePlan_FullMethodName) != 0 ||
					f.gate.count(wirev1.RuntimeService_CloseConnection_FullMethodName) != 0 {
					t.Fatal("known Execution release failure closed its parent or retried")
				}

				fail(operation.release, nil)
				if operation.name == "session run" {
					_, err := f.session.Run(testContext(t))
					if acknowledged && err != nil {
						t.Fatalf("durable Session could not be reused after server committed release: %v", err)
					}

					if !acknowledged && status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("undelivered release should retain the hosted execution: %v", err)
					}

					sibling, err := f.plan.NewSession(testContext(t))
					if err != nil {
						t.Fatalf("release failure invalidated Plan: %v", err)
					}

					if _, err := sibling.Run(testContext(t)); err != nil {
						t.Fatalf("release failure prevented sibling execution: %v", err)
					}
				}

				if _, err := f.remote.Run(testContext(t), api.Source{Content: "RETURN 3"}); err != nil {
					t.Fatalf("release failure invalidated Runtime: %v", err)
				}
			})
		}
	}
}
