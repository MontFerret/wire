package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
)

func TestConnectionCapacityIsRetainedThroughCleanupAndShutdownRejectsAdmission(t *testing.T) {
	entered := make(chan struct{})
	finish := make(chan struct{})
	hosted := &spyPlan{close: func() error {
		close(entered)
		<-finish

		return nil
	}}
	registry := NewConnectionRegistry(1, testLimits().resources())

	connection, err := registry.Open()
	if err != nil {
		t.Fatal(err)
	}

	_, err = CompilePlan(testContext(t), &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return hosted, nil
	}}, connection.Resources(), api.Source{Content: "RETURN 1"}, false)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	ctx := testContext(t)
	go func() { result <- registry.CloseConnection(ctx, connection.ID()) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("cleanup did not reach the hosted plan")
	}

	if _, err := registry.Open(); !hasCategory(err, ErrorKindResourceExhausted) {
		t.Fatalf("closing connection released capacity early: %v", err)
	}

	close(finish)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection cleanup did not settle")
	}

	if _, err := registry.Open(); err != nil {
		t.Fatalf("settled connection retained capacity: %v", err)
	}

	if err := registry.Close(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := registry.Open(); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatalf("shutdown accepted a new logical connection: %v", err)
	}

	_, _, closes := hosted.snapshot()
	if closes != 1 {
		t.Fatalf("hosted plan closed %d times", closes)
	}
}

func TestRegistryShutdownCancelsAllScopesBeforeWaitingAndRetainsFailures(t *testing.T) {
	registry := NewConnectionRegistry(2, testLimits().resources())
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	failure := errors.New("hosted close failed")
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var connections []*Connection
	for range 2 {
		connection, err := registry.Open()
		if err != nil {
			t.Fatal(err)
		}

		connections = append(connections, connection)
		plan := &spyPlan{close: func() error { entered <- struct{}{}; <-release; return failure }}

		_, err = CompilePlan(testContext(t), &spyRuntime{compile: func(context.Context, api.Source, bool) (api.Plan, error) { return plan, nil }}, connection.Resources(), api.NewAnonymousSource("RETURN 1"), false)
		if err != nil {
			t.Fatal(err)
		}
	}

	result := make(chan error, 1)
	go func() { result <- registry.Close(context.Background()) }()
	for range 2 {
		select {
		case <-entered:
		case <-testContext(t).Done():
			t.Fatal("not all scopes began cleanup")
		}
	}

	for _, connection := range connections {
		if connection.Context().Err() == nil {
			t.Fatal("scope not cancelled")
		}
	}

	if _, err := registry.Open(); !hasCategory(err, ErrorKindInvalidState) {
		t.Fatal(err)
	}

	once.Do(func() { close(release) })
	select {
	case err := <-result:
		if !errors.Is(err, failure) {
			t.Fatalf("cleanup error lost after removal: %v", err)
		}
	case <-testContext(t).Done():
		t.Fatal("shutdown did not settle")
	}
}
