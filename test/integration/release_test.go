package integration_test

import (
	"errors"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/test/integration/harness"
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
				fail := f.gate.Fail

				if acknowledged {
					fail = f.gate.FailResponse
				}

				fail(operation.release, releaseErr)

				if _, err := f.allocate(harness.Context(t), nil); !errors.Is(err, releaseErr) {
					t.Fatalf("successful execution lost its release failure: %v", err)
				}

				if f.gate.Count(operation.release) != 1 || f.gate.Count(operation.parentRelease) != 0 ||
					f.gate.Count(harness.ReleasePlan) != 0 ||
					f.gate.Count(harness.CloseRuntime) != 0 {
					t.Fatal("known Execution release failure closed its parent or retried")
				}

				fail(operation.release, nil)

				if operation.name == "session run" {
					_, err := f.session.Run(harness.Context(t))
					if acknowledged && err != nil {
						t.Fatalf("durable Session could not be reused after server committed release: %v", err)
					}

					if !acknowledged && status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("undelivered release should retain the hosted execution: %v", err)
					}

					sibling, err := f.plan.NewSession(harness.Context(t))
					if err != nil {
						t.Fatalf("release failure invalidated Plan: %v", err)
					}

					if _, err := sibling.Run(harness.Context(t)); err != nil {
						t.Fatalf("release failure prevented sibling execution: %v", err)
					}
				}

				if _, err := f.remote.Run(harness.Context(t), api.Source{Content: "RETURN 3"}); err != nil {
					t.Fatalf("release failure invalidated Runtime: %v", err)
				}
			})
		}
	}
}

func TestKnownResourceCloseFailurePreservesSiblings(t *testing.T) {
	for _, index := range []int{0, 2, 3} {
		operation := allocationOperations()[index]

		for _, acknowledged := range []bool{false, true} {
			t.Run(operation.name+map[bool]string{false: "/undelivered", true: "/lost acknowledgement"}[acknowledged], func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				siblingPlan, err := f.remote.Compile(f.h.Context(), api.Source{Content: "RETURN 2"})
				if err != nil {
					t.Fatal(err)
				}

				sibling, err := siblingPlan.NewSession(f.h.Context())
				if err != nil {
					t.Fatal(err)
				}

				closeHandle, err := f.allocate(f.h.Context(), nil)
				if err != nil {
					t.Fatal(err)
				}

				releaseErr := status.Error(codes.Unavailable, "known resource release failed")
				fail := f.gate.Fail

				if acknowledged {
					fail = f.gate.FailResponse
				}

				fail(operation.release, releaseErr)

				for range 2 {
					if err := closeHandle(); !errors.Is(err, releaseErr) {
						t.Fatalf("close did not retain release error: %v", err)
					}
				}

				if f.gate.Count(operation.release) != 1 || f.gate.Count(operation.parentRelease) != 0 || f.gate.Count(harness.CloseRuntime) != 0 {
					t.Fatal("known Close retried or invalidated its parent")
				}

				fail(operation.release, nil)

				if _, err := sibling.Run(f.h.Context()); err != nil {
					t.Fatalf("known Close failure invalidated sibling: %v", err)
				}

				if _, err := f.remote.Run(f.h.Context(), api.Source{Content: "RETURN 3"}); err != nil {
					t.Fatalf("known Close failure invalidated Runtime: %v", err)
				}
			})
		}
	}
}
