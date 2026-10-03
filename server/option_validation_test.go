package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	gooptions "github.com/ziflex/go-options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type (
	optionValidationCause struct{}
	secretNilCredentials  struct {
		credentials.TransportCredentials
	}
)

func (*optionValidationCause) Error() string { return "custom validation failure" }
func (*secretNilCredentials) String() string { return "private credential description" }

func TestOptionValidationJoinsFailuresBeforeConstruction(t *testing.T) {
	first := errors.New("first option failure")
	second := &optionValidationCause{}
	runtime := &managedRuntime{}
	var calls []string
	setters := 0
	invalid := gooptions.New(func(*config, []string) { setters++ }).Value([]string{"rejected"}).Named("custom option").Validators(
		gooptions.SliceEach[[]string](gooptions.Check(func(string) error {
			calls = append(calls, "second")

			return fmt.Errorf("validator: %w", second)
		})),
	).Build()

	s, err := New(runtime, func(*config) error {
		calls = append(calls, "first")

		return first
	}, nil, invalid, func(cfg *config) error {
		calls = append(calls, "last")
		cfg.runtimeIdentity.Name = "valid"

		return nil
	})
	if s != nil || !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("option failures not retained: server=%v err=%v", s, err)
	}

	var (
		typed      *optionValidationCause
		validation gooptions.ValidationError
	)
	if !errors.As(err, &typed) || typed != second || !errors.As(err, &validation) || validation.Field != "custom option" || !validation.OmitValue || validation.Value != "" {
		t.Fatalf("error chains lost: %v", err)
	}

	message := err.Error()

	firstAt, nilAt, secondAt := strings.Index(message, first.Error()), strings.Index(message, "server option must not be nil"), strings.Index(message, "custom option")
	if firstAt < 0 || nilAt <= firstAt || secondAt <= nilAt {
		t.Fatalf("failure order changed: %v", err)
	}

	if strings.Join(calls, ",") != "first,second,last" || setters != 0 {
		t.Fatalf("options or failed setter invoked incorrectly: calls=%v setters=%d", calls, setters)
	}

	if runtime.calls.Load() != 0 || runtime.closes.Load() != 0 {
		t.Fatal("invalid options touched the borrowed runtime")
	}
}

func TestRunOptionValidationCollectsErrorsWithoutStarting(t *testing.T) {
	runtime := &managedRuntime{}
	s := newManagedServer(t, runtime)
	listens := 0
	s.listen = func(context.Context, string, string) (net.Listener, error) {
		listens++

		return nil, errors.New("unexpected listen")
	}
	first, last := errors.New("first run option failure"), errors.New("last run option failure")
	var calls []string

	err := s.Run(managedContext(t), "127.0.0.1:0", func(*runConfig) error {
		calls = append(calls, "first")

		return first
	}, WithShutdownTimeout(0), nil, WithShutdownTimeout(-time.Nanosecond), WithShutdownTimeout(time.Second), func(cfg *runConfig) error {
		calls = append(calls, "last")

		if cfg.shutdownTimeout != time.Second {
			t.Errorf("last valid override lost: %v", cfg.shutdownTimeout)
		}

		return last
	})
	if !errors.Is(err, first) || !errors.Is(err, last) || !strings.Contains(err.Error(), "run option must not be nil") {
		t.Fatalf("run failures not joined: %v", err)
	}

	var values []string
	for _, failure := range optionValidationFailures(err) {
		if failure.Field == "shutdown timeout" {
			values = append(values, failure.Value)
		}
	}

	if strings.Join(values, ",") != "0s,-1ns" {
		t.Fatalf("not every invalid timeout was retained: %v", err)
	}

	if strings.Join(calls, ",") != "first,last" || listens != 0 || runtime.calls.Load() != 0 || runtime.closes.Load() != 0 {
		t.Fatalf("invalid startup performed work: calls=%v listens=%d", calls, listens)
	}

	running := startManaged(t, s)

	remote := managedRemote(t, running.listener)
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	running.cancel()

	if err := managedAwait(t, running.result); err != nil {
		t.Fatal(err)
	}
}

func TestConstructorValidationRetainsFailuresAfterValidOverrides(t *testing.T) {
	runtime := &managedRuntime{}
	identity := RuntimeIdentity{Name: "accepted"}
	creds := credentials.NewTLS(&tls.Config{ServerName: "accepted"})
	limits := DefaultLimits()
	limits.MaxConnections = 0
	var applied config

	s, err := New(runtime, WithRuntimeIdentity(RuntimeIdentity{}), nil, WithLimits(limits), WithUnaryInterceptors(nil), WithStreamInterceptors(nil), WithTransportCredentials(nil), WithRuntimeIdentity(identity), WithLimits(DefaultLimits()), WithTransportCredentials(creds), func(cfg *config) error {
		applied = *cfg

		return nil
	})
	if s != nil || err == nil || !strings.Contains(err.Error(), "server option must not be nil") {
		t.Fatalf("invalid construction accepted: server=%v err=%v", s, err)
	}

	want := []string{"runtime identity name", "limits", `["max connections"]`, "unary interceptors", "[0]", "stream interceptors", "[0]", "transport credentials"}
	var fields []string
	for _, failure := range optionValidationFailures(err) {
		if failure.Field != "" {
			fields = append(fields, failure.Field)
		}
	}

	if strings.Join(fields, ",") != strings.Join(want, ",") {
		t.Fatalf("earlier failures lost: %v", fields)
	}

	if applied.runtimeIdentity != identity || applied.limits != DefaultLimits() || applied.credentials != creds {
		t.Fatal("later valid options did not run")
	}

	if runtime.calls.Load() != 0 || runtime.closes.Load() != 0 {
		t.Fatal("invalid options touched the borrowed runtime")
	}
}

func TestOptionValidatorsDoNotReplaceConfigurationOnFailure(t *testing.T) {
	unary := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		return "original", nil
	}
	stream := func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error { return nil }
	before := config{
		credentials:     credentials.NewTLS(&tls.Config{ServerName: "original"}),
		runtimeIdentity: RuntimeIdentity{Name: "original", Version: "retained", InstanceID: "retained"},
		limits:          DefaultLimits(),
		unary:           []grpc.UnaryServerInterceptor{unary},
		stream:          []grpc.StreamServerInterceptor{stream},
	}
	limits := DefaultLimits()
	limits.MaxConnections, limits.MaxPlansPerConnection, limits.MaxOutboundMessageBytes = 0, -1, 0
	var creds *secretNilCredentials
	for _, test := range []struct {
		name   string
		option Option
		fields []string
		values map[string]string
	}{
		{name: "credentials", option: WithTransportCredentials(creds), fields: []string{"transport credentials"}},
		{name: "identity", option: WithRuntimeIdentity(RuntimeIdentity{Version: "private identity version", InstanceID: "private identity instance"}), fields: []string{"runtime identity name"}},
		{
			name: "limits", option: WithLimits(limits),
			fields: []string{"limits", `["max connections"]`, `["max plans per connection"]`, `["max outbound message bytes"]`},
			values: map[string]string{`["max connections"]`: "0", `["max plans per connection"]`: "-1", `["max outbound message bytes"]`: "0"},
		},
		{
			name: "unary", option: WithUnaryInterceptors(nil, unary, nil),
			fields: []string{"unary interceptors", "[0]", "[2]"}, values: map[string]string{"[0]": "<nil>", "[2]": "<nil>"},
		},
		{
			name: "stream", option: WithStreamInterceptors(nil, stream, nil),
			fields: []string{"stream interceptors", "[0]", "[2]"}, values: map[string]string{"[0]": "<nil>", "[2]": "<nil>"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := before

			err := test.option(&cfg)
			if err == nil {
				t.Fatal("invalid option accepted")
			}

			var fields []string
			for _, failure := range optionValidationFailures(err) {
				if failure.Field == "" {
					continue
				}

				fields = append(fields, failure.Field)
				if test.name == "credentials" && (failure.OmitValue || failure.Value != "<nil>") {
					t.Fatalf("validation included an unrejected value: %+v", failure)
				}

				if test.name == "identity" && (failure.OmitValue || failure.Value != `""`) {
					t.Fatalf("validation included unrelated identity metadata: %+v", failure)
				}

				if test.values != nil && (!failure.OmitValue || failure.Value != "") {
					t.Fatalf("collection wrapper included an aggregate value: %+v", failure)
				}

				if wantValue, ok := test.values[failure.Field]; ok {
					var rejected gooptions.ValidationError
					if !errors.As(failure.Reason, &rejected) || rejected.Field != "" || rejected.OmitValue || rejected.Value != wantValue {
						t.Fatalf("rejected value lost at %s: %+v", failure.Field, failure)
					}
				}
			}

			want := slices.Clone(test.fields)
			if test.name == "limits" && len(fields) == len(want) {
				slices.Sort(fields[1:])
				slices.Sort(want[1:])
			}

			if !slices.Equal(fields, want) {
				t.Fatalf("validation fields changed: %v", fields)
			}

			if cfg.credentials != before.credentials || cfg.runtimeIdentity != before.runtimeIdentity || cfg.limits != before.limits || len(cfg.unary) != 1 || len(cfg.stream) != 1 {
				t.Fatal("failed option partially replaced configuration")
			}

			for _, secret := range []string{"private credential description", "private identity version", "private identity instance"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("unrelated private value disclosed: %v", err)
				}
			}
		})
	}

	for _, invalid := range []time.Duration{0, -time.Nanosecond} {
		cfg := runConfig{shutdownTimeout: 30 * time.Second}

		err := WithShutdownTimeout(invalid)(&cfg)
		if err == nil || cfg.shutdownTimeout != 30*time.Second {
			t.Fatalf("invalid override replaced default: timeout=%v err=%v", cfg.shutdownTimeout, err)
		}
	}
}

func TestLimitsValidationReportsEveryInvalidField(t *testing.T) {
	cfg := config{limits: DefaultLimits()}

	err := WithLimits(Limits{})(&cfg)
	if err == nil || cfg.limits != DefaultLimits() {
		t.Fatalf("invalid limits applied: config=%v err=%v", cfg.limits, err)
	}

	want := []string{"limits", `["max connections"]`, `["max plans per connection"]`, `["max sessions per connection"]`, `["max executions per connection"]`, `["max debug sessions per connection"]`, `["max watchers per resource"]`, `["max breakpoints per debug session"]`, `["max inbound message bytes"]`, `["max outbound message bytes"]`}
	var fields []string
	for _, failure := range optionValidationFailures(err) {
		if failure.Field == "" {
			continue
		}

		fields = append(fields, failure.Field)
		if !failure.OmitValue || failure.Value != "" {
			t.Fatalf("collection wrapper included an aggregate value: %+v", failure)
		}

		if failure.Field != "limits" {
			var rejected gooptions.ValidationError
			if !errors.As(failure.Reason, &rejected) || rejected.Field != "" || rejected.OmitValue || rejected.Value != "0" {
				t.Fatalf("invalid field value lost: %+v", failure)
			}
		}
	}

	if len(fields) == len(want) {
		slices.Sort(fields[1:])
		slices.Sort(want[1:])
	}

	if !slices.Equal(fields, want) {
		t.Fatalf("not every field was reported: %v", fields)
	}
}

func TestOptionValidationPreservesAcceptedBoundaryValues(t *testing.T) {
	identity := RuntimeIdentity{Name: " \t", Version: "opaque version", InstanceID: "opaque instance"}
	cfg := config{limits: DefaultLimits()}
	for _, option := range []Option{WithRuntimeIdentity(identity), WithUnaryInterceptors(), WithStreamInterceptors()} {
		if err := option(&cfg); err != nil {
			t.Fatal(err)
		}
	}

	if cfg.runtimeIdentity != identity || cfg.limits != DefaultLimits() || len(cfg.unary) != 0 || len(cfg.stream) != 0 {
		t.Fatal("accepted values or defaults changed")
	}

	run := runConfig{shutdownTimeout: 30 * time.Second}
	if err := WithShutdownTimeout(time.Nanosecond)(&run); err != nil || run.shutdownTimeout != time.Nanosecond {
		t.Fatalf("smallest positive timeout rejected: %v", err)
	}
}

// Traverse the standard error chain to inspect every joined validation failure;
// errors.As alone selects only the first matching node.
func optionValidationFailures(err error) []gooptions.ValidationError {
	var failures []gooptions.ValidationError
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}

		if validation, ok := err.(gooptions.ValidationError); ok { //nolint:errorlint // Match this node only; errors.As would also match descendants.
			failures = append(failures, validation)
		}

		switch wrapped := err.(type) { //nolint:errorlint // Select this node's unwrap shape without following descendants.
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)

	return failures
}
