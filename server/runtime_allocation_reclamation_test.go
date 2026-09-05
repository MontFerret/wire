package server_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeLostAllocationReclaimsNearestParentAndPreservesSiblings(t *testing.T) {
	for _, operation := range allocationOperations() {
		for _, outcome := range []string{"deadline", "unavailable", "oversized", "transport internal", "malformed"} {
			t.Run(operation.name+"/"+outcome, func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				root := operation.parentRelease == wirev1.RuntimeService_CloseConnection_FullMethodName
				var sibling api.Session
				if !root {
					parent := f.plan
					if operation.name != "session run" {
						var err error
						parent, err = f.remote.Compile(testContext(t), api.Source{Content: "RETURN 2"})
						if err != nil {
							t.Fatal(err)
						}
					}

					var err error
					sibling, err = parent.NewSession(testContext(t))
					if err != nil {
						t.Fatal(err)
					}
				}

				f.gate.arm(operation.method, outcome)
				result := make(chan error, 1)
				go func() {
					_, err := f.allocate(testContext(t), nil)
					result <- err
				}()
				f.awaitCommitted()
				close(f.gate.deliver)
				err := f.awaitResult(result)
				if err == nil {
					t.Fatal("lost or malformed allocation response succeeded")
				}

				if outcome == "deadline" && status.Code(err) != codes.DeadlineExceeded {
					t.Fatalf("allocation deadline was lost: %v", err)
				}

				if calls := f.gate.count(operation.parentRelease); calls != 1 {
					t.Fatalf("parent release calls = %d, want 1", calls)
				}

				if root {
					if _, err := f.remote.Run(testContext(t), api.Source{Content: "RETURN 1"}); !errors.Is(err, client.ErrClosed) {
						t.Fatalf("indeterminate root allocation left Runtime usable: %v", err)
					}

					f.assertAllClosed()
				} else {
					if f.gate.count(wirev1.RuntimeService_CloseConnection_FullMethodName) != 0 {
						t.Fatal("successful narrow cleanup destroyed Runtime")
					}

					// Verify hosted reclamation now, before fixture Runtime teardown
					// could hide a failure to recursively release the narrow parent.
					f.assertNarrowParentClosed()

					if operation.name == "session run" {
						if _, err := f.session.Run(testContext(t)); !errors.Is(err, client.ErrClosed) {
							t.Fatalf("Session parent remained usable: %v", err)
						}
					} else if _, err := f.plan.NewSession(testContext(t)); !errors.Is(err, client.ErrClosed) {
						t.Fatalf("Plan parent remained usable: %v", err)
					}

					if _, err := sibling.Run(testContext(t)); err != nil {
						t.Fatalf("unrelated sibling was invalidated: %v", err)
					}

					if _, err := f.remote.Run(testContext(t), api.Source{Content: "RETURN 1"}); err != nil {
						t.Fatalf("logical Runtime was invalidated: %v", err)
					}
				}

				// This second logical client borrows the same physical connection.
				plan, err := f.env.client.Compile(testContext(t), api.Source{Content: "RETURN 3"}, client.CompileOptions{})
				if err != nil {
					t.Fatalf("caller-owned transport or sibling client was closed: %v", err)
				}

				if err := plan.Close(testContext(t)); err != nil {
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
					siblingPlan, err = f.remote.Compile(testContext(t), api.Source{Content: "RETURN 2"})
					if err != nil {
						t.Fatal(err)
					}
				}

				sibling, err := siblingPlan.NewSession(testContext(t))
				if err != nil {
					t.Fatal(err)
				}

				releaseErr := status.Error(codes.Unavailable, "resource release unavailable")
				fail := f.gate.fail
				if acknowledged {
					fail = f.gate.failResponse
				}

				fail(operation.release, releaseErr)
				f.gate.arm(operation.method, "success")
				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()
				result := make(chan error, 1)
				go func() {
					_, err := f.allocate(ctx, nil)
					result <- err
				}()
				f.awaitCommitted()
				cancel()
				close(f.gate.deliver)
				err = f.awaitResult(result)
				if !errors.Is(err, context.Canceled) || !errors.Is(err, releaseErr) {
					t.Fatalf("cancellation or handle release error was lost: %v", err)
				}

				if f.gate.count(operation.release) != 1 || f.gate.count(operation.parentRelease) != 0 ||
					f.gate.count(wirev1.RuntimeService_CloseConnection_FullMethodName) != 0 {
					t.Fatal("known allocation release failure invalidated an ancestor or retried")
				}

				fail(operation.release, nil)
				if _, err := sibling.Run(testContext(t)); err != nil {
					t.Fatalf("known allocation release failure invalidated its sibling: %v", err)
				}

				if _, err := f.remote.Run(testContext(t), api.Source{Content: "RETURN 3"}); err != nil {
					t.Fatalf("known allocation release failure invalidated Runtime: %v", err)
				}

				if operation.name == "session run" {
					_, err := f.session.Run(testContext(t))
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
	siblingPlan, err := f.remote.Compile(testContext(t), api.Source{Content: "RETURN 2"})
	if err != nil {
		t.Fatal(err)
	}

	sibling, err := siblingPlan.NewSession(testContext(t))
	if err != nil {
		t.Fatal(err)
	}

	parentErr := status.Error(codes.Unavailable, "session release unavailable")
	f.gate.fail(operation.parentRelease, parentErr)
	f.gate.arm(operation.method, "unavailable")
	result := make(chan error, 1)
	go func() {
		_, err := f.allocate(testContext(t), nil)
		result <- err
	}()
	f.awaitCommitted()
	close(f.gate.deliver)
	if err := f.awaitResult(result); !errors.Is(err, parentErr) {
		t.Fatalf("failed Session cleanup was lost: %v", err)
	}

	methods := f.gate.methodSequence()
	sessionRelease := slices.Index(methods, wirev1.SessionService_ReleaseSession_FullMethodName)
	planRelease := slices.Index(methods, wirev1.PlanService_ReleasePlan_FullMethodName)
	if sessionRelease < 0 || planRelease <= sessionRelease ||
		f.gate.count(wirev1.PlanService_ReleasePlan_FullMethodName) != 1 ||
		f.gate.count(wirev1.RuntimeService_CloseConnection_FullMethodName) != 0 {
		t.Fatalf("reclamation did not stop after Session then Plan: %v", methods)
	}

	if _, err := f.plan.NewSession(testContext(t)); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("owning Plan was not invalidated: %v", err)
	}

	f.assertNarrowParentClosed()
	f.plans[0].mu.Lock()
	planCloses := f.plans[0].closeCalls
	f.plans[0].mu.Unlock()
	if planCloses != 1 {
		t.Fatalf("Plan fallback returned before hosted Plan cleanup: closes=%d", planCloses)
	}

	if _, err := sibling.Run(testContext(t)); err != nil {
		t.Fatalf("another Plan's Session was invalidated: %v", err)
	}

	if _, err := f.remote.Run(testContext(t), api.Source{Content: "RETURN 3"}); err != nil {
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
				f.gate.fail(operation.parentRelease, parentErr)
				planErr := status.Error(codes.Unavailable, "plan release unavailable")
				if operation.name == "session run" {
					f.gate.fail(wirev1.PlanService_ReleasePlan_FullMethodName, planErr)
				}

				if failConnection {
					f.expectedCloseError = status.Error(codes.Unavailable, "connection release unavailable")
					f.gate.fail(wirev1.RuntimeService_CloseConnection_FullMethodName, f.expectedCloseError)
				}

				f.gate.arm(operation.method, "deadline")
				result := make(chan error, 1)
				go func() {
					_, err := f.allocate(testContext(t), nil)
					result <- err
				}()
				f.awaitCommitted()
				close(f.gate.deliver)
				err := f.awaitResult(result)
				if !errors.Is(err, parentErr) ||
					(operation.name == "session run" && !errors.Is(err, planErr)) ||
					(failConnection && !errors.Is(err, f.expectedCloseError)) {
					t.Fatalf("cleanup errors were lost: %v", err)
				}

				if calls := f.gate.count(wirev1.RuntimeService_CloseConnection_FullMethodName); calls != 1 {
					t.Fatalf("failed parent cleanup did not escalate once: %d", calls)
				}

				f.assertAllClosed()
			})
		}
	}
}

func TestRuntimeRejectedAllocationPreservesParent(t *testing.T) {
	f := newRuntimeAllocationFixture(t, allocationOperations()[2])
	if _, err := f.remote.Compile(testContext(t), api.Source{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid source was not rejected: %v", err)
	}

	if _, err := f.plan.NewSession(testContext(t), api.WithParam("bad", make(chan int))); err == nil {
		t.Fatal("nonportable parameter was accepted")
	}

	f.plans[0].mu.Lock()
	create := f.plans[0].newSession
	f.plans[0].newSession = func(context.Context, apiSessionOptions) (api.Session, error) {
		panic("hosted constructor secret")
	}
	f.plans[0].mu.Unlock()
	if _, err := f.plan.NewSession(testContext(t)); status.Code(err) != codes.Internal {
		t.Fatalf("constructor panic was not contained: %v", err)
	}

	f.plans[0].mu.Lock()
	f.plans[0].newSession = create
	f.plans[0].mu.Unlock()
	session, err := f.plan.NewSession(testContext(t))
	if err != nil {
		t.Fatalf("rejected creation invalidated its parent: %v", err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}
