package lifecycle

import (
	"context"
	"errors"
	"testing"
)

func TestCloseRetainsOneResultWithoutBindingCleanupToAWaiter(t *testing.T) {
	var closeState Close
	if !closeState.Begin() || closeState.Begin() {
		t.Fatal("Begin must commit teardown exactly once")
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := closeState.Wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter did not leave independently: %v", err)
	}

	want := errors.New("retained result")
	closeState.Finish(want)
	if err := closeState.Wait(context.Background()); !errors.Is(err, want) {
		t.Fatalf("cleanup result was not retained: %v", err)
	}
}
