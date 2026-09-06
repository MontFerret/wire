package integration_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
	"github.com/MontFerret/wire/test/integration/harness"
)

func TestRuntimeAndSessionOutputRoundTrip(t *testing.T) {
	for _, direct := range []bool{true, false} {
		for _, test := range []struct {
			name   string
			output api.Output
			err    error
		}{
			{name: "zero"},
			{name: "empty", output: api.Output{ContentType: "text/plain", Content: []byte{}}},
			{name: "text", output: api.Output{ContentType: "text/plain; charset=utf-8", Content: []byte("héllo")}},
			{name: "binary", output: api.Output{ContentType: "application/octet-stream", Content: []byte{0, 255, 3, 0}}},
			{name: "partial", output: api.Output{ContentType: "application/json", Content: []byte(`{"partial":true}`)}, err: errors.New("host secret")},
		} {
			t.Run(map[bool]string{true: "runtime/", false: "session/"}[direct]+test.name, func(t *testing.T) {
				wantContent := bytes.Clone(test.output.Content)
				h := harness.New(t, harness.WithBehavior(harness.RuntimeBehavior{
					Run: func(context.Context, api.Source, harness.SessionOptions) (api.Output, error) {
						return test.output, test.err
					},
					Plan: harness.PlanBehavior{Session: func(harness.SessionOptions) harness.SessionBehavior {
						return harness.SessionBehavior{Run: func(context.Context, int) (api.Output, error) { return test.output, test.err }}
					}},
				}))
				src := api.Source{Name: "results.fql", Content: "RETURN @input"}
				run := func() (api.Output, error) { return h.Runtime().Run(h.Context(), src) }

				if !direct {
					plan, err := h.Runtime().Compile(h.Context(), src)
					if err != nil {
						t.Fatal(err)
					}

					session, err := plan.NewSession(h.Context())
					if err != nil {
						t.Fatal(err)
					}

					run = func() (api.Output, error) { return session.Run(h.Context()) }
				}

				for range 2 {
					output, err := run()
					if test.err == nil && err != nil {
						t.Fatal(err)
					}

					if test.err != nil {
						var remote *failure.Failure
						if !errors.As(err, &remote) || remote.Category != failure.CategoryExecution {
							t.Fatalf("execution failure changed: %v", err)
						}
					}

					if output.ContentType != test.output.ContentType || !bytes.Equal(output.Content, wantContent) {
						t.Fatalf("output = %#v, want %#v", output, test.output)
					}

					if len(output.Content) > 0 {
						output.Content[0] ^= 255
					}

					if !bytes.Equal(test.output.Content, wantContent) {
						t.Fatal("returned output aliases hosted output bytes")
					}
				}

				snapshot := h.RuntimeSpy().Recorder().Snapshot()

				if direct {
					if len(snapshot.OfKind("plan")) != 0 || len(snapshot.OfKind("session")) != 0 || snapshot.Count(h.RuntimeSpy().ID(), "Run") != 2 {
						t.Fatalf("direct Run was composed from other operations: %+v", snapshot)
					}

					for _, call := range snapshot.Calls {
						if call.Method == "Run" && call.Source != src {
							t.Fatalf("source changed: %+v", call)
						}
					}
				}
			})
		}
	}
}

func TestSessionOptionsRoundTrip(t *testing.T) {
	portable := map[string]any{"null": nil, "bool": true, "integer": int64(-9223372036854775807), "float": 3.5, "text": "héllo", "bytes": []byte{0, 255}, "array": []any{int64(2), map[string]any{"key": "value"}}, "object": map[string]any{"nested": []any{false, nil}}}

	for _, mode := range []string{"runtime", "session", "debugger"} {
		for _, test := range []struct {
			name    string
			options []api.SessionOption
			want    map[string]any
		}{
			{"empty", []api.SessionOption{nil, api.WithParams(map[string]any{})}, map[string]any{}},
			{"single", []api.SessionOption{api.WithParam("input", int64(42))}, map[string]any{"input": int64(42)}},
			{"portable", []api.SessionOption{api.WithParams(portable)}, portable},
			{"overrides", []api.SessionOption{api.WithParam("a", int64(1)), api.WithParams(map[string]any{"a": int64(2), "b": true}), api.WithParam("a", int64(3))}, map[string]any{"a": int64(3), "b": true}},
		} {
			t.Run(mode+"/"+test.name, func(t *testing.T) {
				h := harness.New(t)
				calls := 0
				options := append(append([]api.SessionOption(nil), test.options...), func(options api.SessionOptions) error {
					calls++

					return options.SetOutputContentType("application/custom")
				})
				method := "Run"

				if mode == "runtime" {
					if _, err := h.Runtime().Run(h.Context(), api.Source{Name: "options.fql", Content: "RETURN @input"}, options...); err != nil {
						t.Fatal(err)
					}
				} else {
					plan, err := h.Runtime().CompileDebug(h.Context(), api.Source{Content: "RETURN @input"})
					if err != nil {
						t.Fatal(err)
					}

					method = "NewSession"

					if mode == "session" {
						_, err = plan.NewSession(h.Context(), options...)
					} else {
						method = "NewDebugSession"
						_, err = plan.NewDebugSession(h.Context(), options...)
					}

					if err != nil {
						t.Fatal(err)
					}
				}

				if calls != 1 {
					t.Fatalf("option applied %d times", calls)
				}

				found := 0

				for _, call := range h.RuntimeSpy().Recorder().Snapshot().Calls {
					if call.Method == method {
						found++

						if call.Options.ContentType != "application/custom" || !reflect.DeepEqual(call.Options.Params, test.want) {
							t.Fatalf("options changed: %+v", call.Options)
						}
					}
				}

				if found != 1 {
					t.Fatalf("hosted calls=%d", found)
				}
			})
		}
	}
}

func TestRuntimeCloseBorrowsTransportAndHostedRuntime(t *testing.T) {
	h := harness.New(t)

	other, err := h.OpenRuntime()
	if err != nil {
		t.Fatal(err)
	}

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	if err := h.Runtime().Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := h.Runtime().Run(h.Context(), api.Source{Content: "RETURN 1"}); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("closed Runtime error=%v", err)
	}

	if _, err := other.Run(h.Context(), api.Source{Content: "RETURN 2"}); err != nil {
		t.Fatalf("sibling logical Runtime unusable: %v", err)
	}

	fresh, err := h.OpenRuntime()
	if err != nil {
		t.Fatalf("borrowed transport unusable: %v", err)
	}

	if _, err := fresh.Run(h.Context(), api.Source{Content: "RETURN 3"}); err != nil {
		t.Fatal(err)
	}

	if err := h.Shutdown(); err != nil {
		t.Fatal(err)
	}

	if got := h.RuntimeSpy().Recorder().Snapshot().Count(h.RuntimeSpy().ID(), "Close"); got != 0 {
		t.Fatalf("hosted Runtime closed %d times", got)
	}
}
