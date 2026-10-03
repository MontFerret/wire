package client

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	gooptions "github.com/ziflex/go-options"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	inspectedCredentials struct {
		credentials.TransportCredentials
		inspections atomic.Int64
	}
	nilTransportCredentials struct {
		credentials.TransportCredentials
	}
	nilCallCredentials struct{ credentials.PerRPCCredentials }
	callCredentials    struct{ name string }
)

func (c *inspectedCredentials) Info() credentials.ProtocolInfo {
	c.inspections.Add(1)

	return c.TransportCredentials.Info()
}

func (c *nilTransportCredentials) String() string { return "credential-secret" }
func (c *nilCallCredentials) String() string      { return "credential-secret" }
func (c *callCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{c.name: "value"}, nil
}
func (*callCredentials) RequireTransportSecurity() bool { return false }

func TestConstructorInputValidation(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	var nilTransport *nilTransportCredentials
	var nilCalls *nilCallCredentials
	for _, test := range []struct {
		name    string
		ctx     context.Context
		target  string
		options []Option
		cause   error
	}{
		{name: "nil context", target: "127.0.0.1:1"},
		{name: "cancelled context", ctx: cancelled, target: "127.0.0.1:1", cause: context.Canceled},
		{name: "empty target", ctx: t.Context()},
		{name: "whitespace target", ctx: t.Context(), target: " \t\n"},
		{name: "nil option", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{nil}},
		{name: "nil transport", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{WithTransportCredentials(nil)}},
		{name: "typed nil transport", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{WithTransportCredentials(nilTransport)}},
		{name: "nil calls", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{WithPerRPCCredentials(nil)}},
		{name: "typed nil calls", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{WithPerRPCCredentials(nilCalls)}},
		{name: "insecure then custom", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{WithInsecure(), WithTransportCredentials(insecure.NewCredentials())}},
		{name: "custom then insecure", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{WithTransportCredentials(insecure.NewCredentials()), WithInsecure()}},
		{name: "invalid overridden", ctx: t.Context(), target: "127.0.0.1:1", options: []Option{WithTransportCredentials(nilTransport), WithTransportCredentials(insecure.NewCredentials())}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := &inspectedCredentials{TransportCredentials: insecure.NewCredentials()}
			options := append(append([]Option(nil), test.options...), WithTransportCredentials(observed))

			remote, err := New(test.ctx, test.target, options...)
			if remote != nil || err == nil {
				t.Fatalf("invalid constructor returned %v, %v", remote, err)
			}

			if test.cause != nil && !errors.Is(err, test.cause) {
				t.Fatalf("lost context cause: %v", err)
			}

			if observed.inspections.Load() != 0 {
				t.Fatal("invalid input reached channel construction")
			}

			if strings.Contains(err.Error(), "credential-secret") {
				t.Fatalf("validation formatted credential contents: %v", err)
			}
		})
	}
}

func TestTransportOptionsOrderingAndReuse(t *testing.T) {
	first := credentials.NewTLS(nil)
	last := insecure.NewCredentials()
	firstCall, lastCall := &callCredentials{name: "first"}, &callCredentials{name: "last"}
	options := []Option{WithTransportCredentials(first), WithPerRPCCredentials(firstCall), WithTransportCredentials(last), WithPerRPCCredentials(lastCall)}
	for range 2 {
		configured, err := configure(options)
		if err != nil {
			t.Fatal(err)
		}

		if configured.transport != last || len(configured.perRPC) != 2 || configured.perRPC[0] != firstCall || configured.perRPC[1] != lastCall {
			t.Fatalf("options changed order or accumulated state: %+v", configured)
		}
	}

	configured, err := configure([]Option{WithInsecure(), WithInsecure()})
	if err != nil || !configured.insecure {
		t.Fatalf("repeated insecure option: %v", err)
	}

	var nilTransport *nilTransportCredentials
	_, err = configure([]Option{WithTransportCredentials(nilTransport), WithTransportCredentials(last)})

	var invalid gooptions.ValidationError
	if !errors.As(err, &invalid) || invalid.Field != "transport credentials" {
		t.Fatalf("invalid override lost named validation: %v", err)
	}
}

func TestTransportOptionsApplyOnceAndJoinFailures(t *testing.T) {
	firstErr, lastErr := errors.New("first option"), errors.New("last option")
	var applied []int
	options := []Option{
		func(*config) error {
			applied = append(applied, 1)

			return firstErr
		},
		func(*config) error {
			applied = append(applied, 2)

			return nil
		},
		func(*config) error {
			applied = append(applied, 3)

			return lastErr
		},
	}

	_, err := configure(options)
	if !errors.Is(err, firstErr) || !errors.Is(err, lastErr) {
		t.Fatalf("option validation lost independent failures: %v", err)
	}

	if len(applied) != 3 || applied[0] != 1 || applied[1] != 2 || applied[2] != 3 {
		t.Fatalf("options were repeated, skipped, or reordered: %v", applied)
	}
}

func TestStartupCancellationDuringOptionsPreventsChannelCreation(t *testing.T) {
	ctx, cancel := context.WithCancel(testClientContext(t))
	defer cancel()
	observed := &inspectedCredentials{TransportCredentials: insecure.NewCredentials()}

	remote, err := New(ctx, "127.0.0.1:1", func(*config) error {
		cancel()

		return nil
	}, WithTransportCredentials(observed))
	if remote != nil || !errors.Is(err, context.Canceled) || observed.inspections.Load() != 0 {
		t.Fatalf("startup cancellation reached channel creation: runtime=%v err=%v inspections=%d", remote, err, observed.inspections.Load())
	}
}
