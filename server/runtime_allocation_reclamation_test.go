package server_test

import (
	"context"
	"errors"
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

func TestRuntimeCancelledAllocationEscalatesFailedHandleRelease(t *testing.T) {
	for _, operation := range allocationOperations() {
		t.Run(operation.name, func(t *testing.T) {
			f := newRuntimeAllocationFixture(t, operation)
			releaseErr := status.Error(codes.Unavailable, "resource release unavailable")
			f.gate.fail(operation.release, releaseErr)
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
			err := f.awaitResult(result)
			if !errors.Is(err, context.Canceled) || !errors.Is(err, releaseErr) {
				t.Fatalf("cancellation or handle release error was lost: %v", err)
			}

			if f.gate.count(operation.release) != 1 || f.gate.count(operation.parentRelease) != 1 {
				t.Fatal("failed handle release did not reclaim its parent exactly once")
			}
		})
	}
}

func TestRuntimeLostAllocationEscalatesFailedParentCleanup(t *testing.T) {
	for _, index := range []int{2, 4} {
		operation := allocationOperations()[index]
		for _, failConnection := range []bool{false, true} {
			t.Run(operation.name+map[bool]string{false: "/connection release", true: "/Connect stream"}[failConnection], func(t *testing.T) {
				f := newRuntimeAllocationFixture(t, operation)
				parentErr := status.Error(codes.Unavailable, "parent release unavailable")
				f.gate.fail(operation.parentRelease, parentErr)
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
				if !errors.Is(err, parentErr) || (failConnection && !errors.Is(err, f.expectedCloseError)) {
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
