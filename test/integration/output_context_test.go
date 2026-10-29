package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestOutputCallbackPreservesConsumerValuesAndEarliestDeadline(t *testing.T) {
	for _, test := range []struct {
		name        string
		invocation  time.Duration
		consumption time.Duration
	}{
		{"neither", 0, 0},
		{"invocation only", time.Minute, 0},
		{"consumer only", 0, time.Minute},
		{"invocation earlier", time.Minute, 2 * time.Minute},
		{"consumer earlier", 2 * time.Minute, time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			type contextKey struct{}
			h := harness.New(t)
			invocation, cancel := context.WithCancel(t.Context())
			defer cancel()
			consumer, stop := context.WithCancel(context.WithValue(t.Context(), contextKey{}, "consumer"))
			defer stop()

			if test.invocation != 0 {
				var timeout context.CancelFunc
				invocation, timeout = context.WithTimeout(invocation, test.invocation)
				defer timeout()
			}

			if test.consumption != 0 {
				var timeout context.CancelFunc
				consumer, timeout = context.WithTimeout(consumer, test.consumption)
				defer timeout()
			}

			output, err := h.Runtime().Run(invocation, api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			called := false

			err = output.Consume(consumer, func(ctx context.Context, _ []byte) error {
				called = true

				want, present := invocation.Deadline()
				if deadline, ok := consumer.Deadline(); ok && (!present || deadline.Before(want)) {
					want, present = deadline, true
				}

				actual, ok := ctx.Deadline()
				if ok != present || actual != want || ctx.Value(contextKey{}) != "consumer" {
					t.Error("callback lost consumer values or earliest deadline")
				}

				return nil
			})
			if err != nil || !called {
				t.Fatalf("callback=%v error=%v", called, err)
			}

			if err := output.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOutputCallbackDeadlineIdentity(t *testing.T) {
	for _, invocationDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "consumer", true: "invocation"}[invocationDeadline], func(t *testing.T) {
			h := harness.New(t)
			deadline, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			invocation, consumer := t.Context(), deadline

			if invocationDeadline {
				invocation, consumer = deadline, t.Context()
			}

			output, err := h.Runtime().Run(invocation, api.Source{})
			if err != nil {
				t.Fatal(err)
			}

			err = output.Consume(consumer, func(ctx context.Context, _ []byte) error {
				<-ctx.Done()

				return ctx.Err()
			})
			if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, api.ErrOutputClosed) {
				t.Fatalf("deadline identity=%v", err)
			}

			if err := output.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
