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
