package core

import (
	"context"
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
