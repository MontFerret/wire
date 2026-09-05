package integration_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/test/integration/harness"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeLostAllocationReclaimsNearestParentAndPreservesSiblings(t *testing.T) {
	for _, operation := range allocationOperations() {
		for _, outcome := range []harness.Outcome{harness.LostDeadline, harness.LostUnavailable, harness.LostOversized, harness.LostDecode, harness.Malformed} {
			t.Run(operation.name+"/"+string(outcome), func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				root := operation.parentRelease == harness.CloseRuntime
				var sibling api.Session

				if !root {
					parent := f.plan

					if operation.name != "session run" {
						var err error
						parent, err = f.remote.Compile(harness.Context(t), api.Source{Content: "RETURN 2"})
						if err != nil {
							t.Fatal(err)
						}
					}

					var err error
					sibling, err = parent.NewSession(harness.Context(t))
					if err != nil {
						t.Fatal(err)
					}
				}

				f.reply = f.gate.Arm(operation.method, outcome)
				result := make(chan error, 1)
				go func() {
					_, err := f.allocate(harness.Context(t), nil)
					result <- err
				}()
				f.awaitCommitted()
				f.reply.Deliver()
				err := f.awaitResult(result)
				if err == nil {
					t.Fatal("lost or malformed allocation response succeeded")
				}

				if outcome == harness.LostDeadline && status.Code(err) != codes.DeadlineExceeded {
					t.Fatalf("allocation deadline was lost: %v", err)
				}

				if calls := f.gate.Count(operation.parentRelease); calls != 1 {
					t.Fatalf("parent release calls = %d, want 1", calls)
				}

				if root {
					if _, err := f.remote.Run(harness.Context(t), api.Source{Content: "RETURN 1"}); !errors.Is(err, client.ErrClosed) {
						t.Fatalf("indeterminate root allocation left Runtime usable: %v", err)
					}

					f.assertAllClosed()
				} else {
					if f.gate.Count(harness.CloseRuntime) != 0 {
						t.Fatal("successful narrow cleanup destroyed Runtime")
					}

					// Verify hosted reclamation now, before fixture Runtime teardown
					// could hide a failure to recursively release the narrow parent.
					f.assertNarrowParentClosed()

					if operation.name == "session run" {
						if _, err := f.session.Run(harness.Context(t)); !errors.Is(err, client.ErrClosed) {
							t.Fatalf("Session parent remained usable: %v", err)
						}
					} else if _, err := f.plan.NewSession(harness.Context(t)); !errors.Is(err, client.ErrClosed) {
						t.Fatalf("Plan parent remained usable: %v", err)
					}

					if _, err := sibling.Run(harness.Context(t)); err != nil {
						t.Fatalf("unrelated sibling was invalidated: %v", err)
					}

					if _, err := f.remote.Run(harness.Context(t), api.Source{Content: "RETURN 1"}); err != nil {
						t.Fatalf("logical Runtime was invalidated: %v", err)
					}
				}

				// This second logical client borrows the same physical connection.
				plan, err := f.other.Compile(harness.Context(t), api.Source{Content: "RETURN 3"})
				if err != nil {
					t.Fatalf("caller-owned transport or sibling client was closed: %v", err)
				}

				if err := plan.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRuntimeCancelledKnownAllocationPreservesParentsOnReleaseFailure(t *testing.T) {
	for _, operation := range allocationOperations() {
		for _, acknowledged := range []bool{false, true} {
			t.Run(operation.name+map[bool]string{false: "/failed delivery", true: "/lost acknowledgement"}[acknowledged], func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				siblingPlan := f.plan
				if siblingPlan == nil {
					var err error
					siblingPlan, err = f.remote.Compile(harness.Context(t), api.Source{Content: "RETURN 2"})
					if err != nil {
						t.Fatal(err)
					}
				}

				sibling, err := siblingPlan.NewSession(harness.Context(t))
				if err != nil {
					t.Fatal(err)
				}

				releaseErr := status.Error(codes.Unavailable, "resource release unavailable")
				fail := f.gate.Fail

				if acknowledged {
					fail = f.gate.FailResponse
				}

				fail(operation.release, releaseErr)
				f.reply = f.gate.Arm(operation.method, harness.Deliver)
				ctx, cancel := context.WithCancel(harness.Context(t))
				defer cancel()
				result := make(chan error, 1)
				go func() {
					_, err := f.allocate(ctx, nil)
					result <- err
				}()
				f.awaitCommitted()
				cancel()
				f.reply.Deliver()
				err = f.awaitResult(result)
				if !errors.Is(err, context.Canceled) || !errors.Is(err, releaseErr) {
					t.Fatalf("cancellation or handle release error was lost: %v", err)
				}

				if f.gate.Count(operation.release) != 1 || f.gate.Count(operation.parentRelease) != 0 ||
					f.gate.Count(harness.CloseRuntime) != 0 {
					t.Fatal("known allocation release failure invalidated an ancestor or retried")
				}

				fail(operation.release, nil)

				if _, err := sibling.Run(harness.Context(t)); err != nil {
					t.Fatalf("known allocation release failure invalidated its sibling: %v", err)
				}

				if _, err := f.remote.Run(harness.Context(t), api.Source{Content: "RETURN 3"}); err != nil {
					t.Fatalf("known allocation release failure invalidated Runtime: %v", err)
				}

				if operation.name == "session run" {
					_, err := f.session.Run(harness.Context(t))
					if acknowledged && err != nil {
						t.Fatalf("Session could not run after committed execution release: %v", err)
					}

					if !acknowledged && status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("undelivered release should leave the hosted execution active, not invalidate Session: %v", err)
					}
				}
			})
		}
	}
}

func TestRuntimeLostExecutionTriesPlanBeforeRuntime(t *testing.T) {
	operation := allocationOperations()[4]
	f := newRuntimeAllocationFixture(t, operation)
	siblingPlan, err := f.remote.Compile(harness.Context(t), api.Source{Content: "RETURN 2"})
	if err != nil {
		t.Fatal(err)
	}

	sibling, err := siblingPlan.NewSession(harness.Context(t))
	if err != nil {
		t.Fatal(err)
	}

	parentErr := status.Error(codes.Unavailable, "session release unavailable")
	f.gate.Fail(operation.parentRelease, parentErr)
	f.reply = f.gate.Arm(operation.method, harness.LostUnavailable)
	result := make(chan error, 1)
	go func() {
		_, err := f.allocate(harness.Context(t), nil)
		result <- err
	}()
	f.awaitCommitted()
	f.reply.Deliver()

	if err := f.awaitResult(result); !errors.Is(err, parentErr) {
		t.Fatalf("failed Session cleanup was lost: %v", err)
	}

	methods := f.gate.Sequence()
	sessionRelease := slices.Index(methods, harness.ReleaseSession)
	planRelease := slices.Index(methods, harness.ReleasePlan)
	if sessionRelease < 0 || planRelease <= sessionRelease ||
		f.gate.Count(harness.ReleasePlan) != 1 ||
		f.gate.Count(harness.CloseRuntime) != 0 {
		t.Fatalf("reclamation did not stop after Session then Plan: %v", methods)
	}

	if _, err := f.plan.NewSession(harness.Context(t)); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("owning Plan was not invalidated: %v", err)
	}

	f.assertNarrowParentClosed()
	planCloses := f.record.Snapshot().Count(f.record.Snapshot().OfKind("plan")[0].ID, "Close")
	if planCloses != 1 {
		t.Fatalf("Plan fallback returned before hosted Plan cleanup: closes=%d", planCloses)
	}

	if _, err := sibling.Run(harness.Context(t)); err != nil {
		t.Fatalf("another Plan's Session was invalidated: %v", err)
	}

	if _, err := f.remote.Run(harness.Context(t), api.Source{Content: "RETURN 3"}); err != nil {
		t.Fatalf("successful Plan reclamation invalidated Runtime: %v", err)
	}
}

func TestRuntimeLostAllocationEscalatesFailedParentCleanup(t *testing.T) {
	for _, index := range []int{2, 3, 4} {
		operation := allocationOperations()[index]

		for _, failConnection := range []bool{false, true} {
			t.Run(operation.name+map[bool]string{false: "/connection release", true: "/Connect stream"}[failConnection], func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				parentErr := status.Error(codes.Unavailable, "parent release unavailable")
				f.gate.Fail(operation.parentRelease, parentErr)
				planErr := status.Error(codes.Unavailable, "plan release unavailable")

				if operation.name == "session run" {
					f.gate.Fail(harness.ReleasePlan, planErr)
				}

				if failConnection {
					f.expectedCloseError = status.Error(codes.Unavailable, "connection release unavailable")
					f.h.ExpectCleanupError(f.expectedCloseError)
					f.gate.Fail(harness.CloseRuntime, f.expectedCloseError)
				}

				f.reply = f.gate.Arm(operation.method, harness.LostDeadline)
				result := make(chan error, 1)
				go func() {
					_, err := f.allocate(harness.Context(t), nil)
					result <- err
				}()
				f.awaitCommitted()
				f.reply.Deliver()
				err := f.awaitResult(result)
				if !errors.Is(err, parentErr) ||
					(operation.name == "session run" && !errors.Is(err, planErr)) ||
					(failConnection && !errors.Is(err, f.expectedCloseError)) {
					t.Fatalf("cleanup errors were lost: %v", err)
				}

				if calls := f.gate.Count(harness.CloseRuntime); calls != 1 {
					t.Fatalf("failed parent cleanup did not escalate once: %d", calls)
				}

				f.assertAllClosed()
			})
		}
	}
}
