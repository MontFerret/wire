package client

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/MontFerret/api"
)

func TestRemoteOutputCancellationStopsAdmissionDuringCallback(t *testing.T) {
	server := &clientTestServer{outputScripts: []executionOutputScript{{frames: completedOutputFrames("text/plain", []byte("borrowed"))}}}
	remote := openTestRuntime(t, startClientTestServer(t, server))

	output, err := remote.Run(testClientContext(t), api.Source{})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(testClientContext(t))
	defer cancel()
	entered, canceled, unblock := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	result := make(chan error, 1)
	go func() {
		result <- output.Consume(ctx, func(effective context.Context, data []byte) error {
			close(entered)
			<-effective.Done()
			close(canceled)
			<-unblock

			if string(data) != "borrowed" {
				return errors.New("borrowed buffer changed during cancellation")
			}

			return effective.Err()
		})
	}()
	select {
	case <-entered:
	case <-testClientContext(t).Done():
		t.Fatal("callback did not enter")
	}

	if _, err := output.Collect(testClientContext(t)); !errors.Is(err, api.ErrOutputInUse) {
		t.Fatalf("active admission=%v", err)
	}

	cancel()
	select {
	case <-canceled:
	case <-testClientContext(t).Done():
		t.Fatal("callback did not observe cancellation")
	}

	if _, err := output.Collect(testClientContext(t)); !errors.Is(err, api.ErrOutputClosed) {
		t.Fatalf("stopping admission=%v", err)
	}

	_, _, releases, _ := server.callSnapshot()
	if releases != 0 {
		t.Fatal("callback lost its finalization owner")
	}

	release()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || errors.Is(err, api.ErrOutputClosed) {
			t.Fatalf("automatic cancellation=%v", err)
		}
	case <-testClientContext(t).Done():
		t.Fatal("consumption did not finalize")
	}

	if err := output.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, releases, _ = server.callSnapshot()
	if releases != 1 {
		t.Fatalf("release count=%d", releases)
	}
}

func TestRemoteOutputAdmissionDoesNotWaitForCancellationCallback(t *testing.T) {
	for _, source := range []string{"consumption", "lifetime"} {
		t.Run(source, func(t *testing.T) {
			consumption, cancelConsumption := context.WithCancel(testClientContext(t))
			defer cancelConsumption()
			lifetime, cancelLifetime := context.WithCancel(testClientContext(t))
			defer cancelLifetime()
			// No AfterFunc is installed: admission must discover cancellation itself.
			output := &remoteOutput{invocation: testClientContext(t), lifetime: lifetime, consumption: consumption, active: true}

			if source == "consumption" {
				cancelConsumption()
			} else {
				cancelLifetime()
			}

			if err := output.Consume(testClientContext(t), func(context.Context, []byte) error { t.Error("rejected callback ran"); return nil }); !errors.Is(err, api.ErrOutputClosed) {
				t.Fatalf("admission before cancellation callback=%v", err)
			}

			canceled, cancel := context.WithCancel(testClientContext(t))
			cancel()

			if err := output.Consume(canceled, func(context.Context, []byte) error { return nil }); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled admission=%v", err)
			}
		})
	}
}
