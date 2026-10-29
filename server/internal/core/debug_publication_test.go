package core

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
)

func TestDebugPublicationRollbackDetachesBothLifetimes(t *testing.T) {
	lifetime := &callbackLifetime{Context: context.Background(), done: make(chan struct{})}
	store := newResourceStore(lifetime, testLimits().resources())
	cleanupErr := errors.New("debug cleanup failed")
	var cancelRequest context.CancelFunc
	var hosted *spyDebugger
	parent := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
		cancelRequest()

		return hosted, nil
	}}

	plan, err := CompilePlan(testContext(t), &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return parent, nil
	}}, store, api.Source{}, true)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := plan.Release(testContext(t)); err != nil {
			t.Error(err)
		}
	}()

	for range 10 {
		hosted = &spyDebugger{close: func() error { return cleanupErr }}
		var request context.Context
		request, cancelRequest = context.WithCancel(testContext(t))
		created, err := plan.NewDebugSession(request)
		cancelRequest()

		if created != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, cleanupErr) {
			t.Fatalf("creation=%v error=%v", created, err)
		}

		hosted.mu.Lock()
		closes := hosted.closeCalls
		hosted.mu.Unlock()
		store.mu.Lock()
		pending, published, children := store.pending[debugResource], len(store.debugSessions), len(plan.debugSessions)
		store.mu.Unlock()

		if closes != 1 || lifetime.callbacks.Load() != 0 || pending != 0 || published != 0 || children != 0 {
			t.Fatalf("closes=%d callbacks=%d pending=%d published=%d children=%d", closes, lifetime.callbacks.Load(), pending, published, children)
		}
	}
}

func TestCompletedDebugPublicationRetainsDeliveryUntilRelease(t *testing.T) {
	lifetime := &callbackLifetime{Context: context.Background(), done: make(chan struct{})}
	store := newResourceStore(lifetime, testLimits().resources())
	parent := &spyPlan{newDebugSession: func(context.Context, sessionOptions) (debugger.Session, error) {
		return &spyDebugger{start: func(context.Context) (*debugger.Event, error) {
			return &debugger.Event{Reason: debugger.ReasonCompleted, Output: &api.Content{Data: []byte("retained")}}, nil
		}}, nil
	}}

	plan, err := CompilePlan(testContext(t), &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) { return parent, nil }}, store, api.Source{}, true)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := plan.Release(testContext(t)); err != nil {
			t.Error(err)
		}
	}()

	session, err := plan.NewDebugSession(testContext(t))
	if err != nil {
		t.Fatal(err)
	}

	if lifetime.callbacks.Load() != 2 {
		t.Fatalf("registered lifetimes=%d", lifetime.callbacks.Load())
	}

	if _, err := session.Start(testContext(t)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-session.Done():
	case <-testContext(t).Done():
		t.Fatal("debugger did not finish")
	}

	replay, err := session.Watch()
	if err != nil {
		t.Fatal(err)
	}

	replay.Cancel()

	if replay.Current.Snapshot.State != wiredebugger.StateCompleted || replay.Current.Snapshot.Output == nil || string(replay.Current.Snapshot.Output.Data) != "retained" || session.DeliveryContext().Err() != nil || lifetime.callbacks.Load() != 1 {
		t.Fatalf("replay=%v callbacks=%d", replay.Current, lifetime.callbacks.Load())
	}

	if err := session.Release(testContext(t)); err != nil {
		t.Fatal(err)
	}

	if lifetime.callbacks.Load() != 0 || !errors.Is(session.DeliveryContext().Err(), context.Canceled) {
		t.Fatal("release retained delivery lifetime")
	}
}
