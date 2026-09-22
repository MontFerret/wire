package core

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/api"
)

func TestPlanReleaseSettlesAbandonedSessionBeforeReclaimingCapacity(t *testing.T) {
	ctx := testContext(t)
	constructorStarted := make(chan struct{})
	finishConstructor := make(chan struct{})
	closeStarted := make(chan struct{})
	finishClose := make(chan struct{})
	closeErr := errors.New("session cleanup failed")
	hosted := &spySession{close: func() error {
		close(closeStarted)
		select {
		case <-finishClose:
		case <-ctx.Done():
			return ctx.Err()
		}

		return closeErr
	}}
	parent := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		close(constructorStarted)
		select {
		case <-finishConstructor:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return hosted, nil
	}}
	sibling := &spyPlan{newSession: func(context.Context, sessionOptions) (api.Session, error) {
		return &spySession{}, nil
	}}
	runtime := &spyRuntime{compile: func(_ context.Context, source api.Source, _ bool) (api.Plan, error) {
		if source.Name == "parent" {
			return parent, nil
		}

		return sibling, nil
	}}
	limits := testLimits().resources()
	limits.Sessions = 1
	registry := NewConnectionRegistry(1, limits)

	connection, err := registry.Open()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := registry.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})

	plan, err := CompilePlan(ctx, runtime, connection.Resources(), api.Source{Name: "parent", Content: "RETURN 1"}, false)
	if err != nil {
		t.Fatal(err)
	}

	other, err := CompilePlan(ctx, runtime, connection.Resources(), api.Source{Name: "sibling", Content: "RETURN 2"}, false)
	if err != nil {
		t.Fatal(err)
	}

	creation := make(chan error, 1)
	go func() {
		_, err := plan.NewSession(ctx)
		creation <- err
	}()
	select {
	case <-constructorStarted:
	case <-ctx.Done():
		t.Fatal("session constructor did not start")
	}

	release := make(chan error, 1)
	go func() { release <- plan.Release(ctx) }()
	waitPlanClosing(t, plan)
	close(finishConstructor)
	select {
	case <-closeStarted:
	case <-ctx.Done():
		t.Fatal("rejected session was not closed")
	}

	if _, err := other.NewSession(ctx); !hasCategory(err, ErrorKindResourceExhausted) {
		t.Fatalf("abandoned session released its reservation before cleanup: %v", err)
	}

	_, _, planCloses := parent.snapshot()
	if planCloses != 0 {
		t.Fatal("hosted plan closed before its abandoned session")
	}

	close(finishClose)
	select {
	case err := <-creation:
		if !hasCategory(err, ErrorKindPlanNotFound) || !errors.Is(err, closeErr) {
			t.Fatalf("rejected publication lost its lookup or cleanup error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("abandoned session cleanup did not settle")
	}

	select {
	case err := <-release:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("plan release did not settle")
	}

	_, closes := hosted.counts()
	if closes != 1 {
		t.Fatalf("abandoned session closed %d times", closes)
	}

	created, err := other.NewSession(ctx)
	if err != nil {
		t.Fatalf("settled reservation was not reclaimed: %v", err)
	}

	if err := created.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPlanCloseSettlesAdmittedConstructorWithoutCancellingChildren(t *testing.T) {
	for _, temporary := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "temporary"}[temporary], func(t *testing.T) {
			ctx := testContext(t)
			entered, finish := make(chan context.Context, 1), make(chan struct{})
			child := &spySession{}
			cleanupErr := errors.New("plan cleanup")
			hosted := &spyPlan{close: func() error { return cleanupErr }, newSession: func(ctx context.Context, _ sessionOptions) (api.Session, error) {
				entered <- ctx
				select {
				case <-finish:
					return child, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}
			store := newResourceStore(ctx, testLimits().resources())
			runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) { return hosted, nil }}

			plan, err := CompilePlan(ctx, runtime, store, api.Source{}, false)
			if err != nil {
				t.Fatal(err)
			}

			result := make(chan *Session, 1)
			created := make(chan error, 1)
			var run *Execution

			if temporary {
				run, err = plan.Execute(ctx)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				go func() { session, err := plan.NewSession(ctx); result <- session; created <- err }()
			}

			var admitted context.Context
			select {
			case admitted = <-entered:
			case <-ctx.Done():
				t.Fatal("constructor did not enter")
			}

			cancelled, cancel := context.WithCancel(ctx)
			cancel()

			if err := plan.Close(cancelled); !errors.Is(err, context.Canceled) {
				t.Fatalf("close waiter=%v", err)
			}

			if _, err := plan.NewSession(ctx); err == nil {
				t.Fatal("close admitted new constructor")
			}

			if admitted.Err() != nil {
				t.Fatalf("ordinary close cancelled admitted constructor: %v", admitted.Err())
			}

			if _, _, count := hosted.snapshot(); count != 0 {
				t.Fatal("hosted plan closed before admitted constructor")
			}

			close(finish)

			if err := plan.Close(ctx); !errors.Is(err, cleanupErr) {
				t.Fatalf("close result=%v", err)
			}

			if err := plan.Close(ctx); !errors.Is(err, cleanupErr) {
				t.Fatalf("retained result=%v", err)
			}

			if !temporary {
				select {
				case err := <-created:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("constructor did not publish")
				}

				session := <-result

				run, err = session.Execute(ctx)
				if err != nil {
					t.Fatalf("closed ancestor rejected child execution: %v", err)
				}

				select {
				case <-run.done:
				case <-ctx.Done():
					t.Fatal("child execution stalled")
				}

				if err := session.Release(ctx); err != nil {
					t.Fatalf("child inherited plan close error: %v", err)
				}
			} else {
				select {
				case <-run.done:
				case <-ctx.Done():
					t.Fatal("temporary execution stalled")
				}
			}

			if err := plan.Release(ctx); !errors.Is(err, cleanupErr) {
				t.Fatalf("release result=%v", err)
			}

			if _, _, count := hosted.snapshot(); count != 1 {
				t.Fatalf("plan closed %d times", count)
			}

			if _, count := child.counts(); count != 1 {
				t.Fatalf("session closed %d times", count)
			}
		})
	}
}

func TestPlanMetadataAndCleanupErrorsRemainJoined(t *testing.T) {
	metadataErr, closeErr := errors.New("metadata"), errors.New("cleanup")
	hosted := &spyPlan{paramsError: metadataErr, close: func() error { return closeErr }}
	store := newResourceStore(testContext(t), testLimits().resources())
	runtime := &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) { return hosted, nil }}

	plan, err := CompilePlan(testContext(t), runtime, store, api.Source{}, false)
	if plan != nil || !errors.Is(err, metadataErr) || !errors.Is(err, closeErr) {
		t.Fatalf("metadata failure=%v %v", plan, err)
	}

	if _, _, count := hosted.snapshot(); count != 1 {
		t.Fatalf("unpublished plan closed %d times", count)
	}
}
