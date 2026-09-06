package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRuntimeAllocationContextRetainsValuesAndBoundsDetachedAcquisition(t *testing.T) {
	type contextKey struct{}
	parent, cancelParent := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "retained"))
	defer cancelParent()
	before := time.Now()

	allocation, cancel, err := runtimeAllocationContext(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	cancelParent()

	if allocation.Err() != nil || allocation.Value(contextKey{}) != "retained" {
		t.Fatal("allocation lost context values or inherited caller cancellation")
	}

	deadline, present := allocation.Deadline()
	if !present || deadline.Before(before.Add(30*time.Second)) || deadline.After(time.Now().Add(30*time.Second)) {
		t.Fatalf("allocation does not have its own 30-second bound: %v", deadline)
	}

	cancel()

	if !errors.Is(allocation.Err(), context.Canceled) {
		t.Fatal("allocation cancellation did not terminate its context")
	}

	if _, _, err := runtimeAllocationContext(parent); !errors.Is(err, context.Canceled) {
		t.Fatalf("already cancelled caller was detached: %v", err)
	}
}
