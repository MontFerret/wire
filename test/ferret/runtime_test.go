package ferret_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/pkg/failure"
)

func TestRuntimeRun(t *testing.T) {
	h := newHarness(t)

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "value.txt"), []byte("rooted"), 0o600); err != nil {
		t.Fatal(err)
	}

	// This root differs from the engine's default and contains the only copy.
	output, err := h.runtime.Run(h.ctx,
		api.NewSource("direct.fql", `RETURN [@value, TO_STRING(IO::FS::READ("value.txt"))]`),
		api.WithParam("value", "direct"),
		api.WithOutputContentType("application/json"),
		api.WithFSRoot(root),
	)
	if err != nil {
		t.Fatal(err)
	}

	requireJSONOutput(t, output, `["direct","rooted"]`)
}

func TestReusablePlanAndDurableSessions(t *testing.T) {
	h := newHarness(t)

	plan, err := h.runtime.Compile(h.ctx, api.NewSource("params.fql", "RETURN @value"))
	if err != nil {
		t.Fatal(err)
	}

	h.own(plan)
	requireDetachedParams(t, plan)

	first, err := plan.NewSession(h.ctx, api.WithParam("value", "first"), api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	h.own(first)

	second, err := plan.NewSession(h.ctx, api.WithParam("value", "second"), api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	h.own(second)

	firstOutput, err := first.Run(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	requireJSONOutput(t, firstOutput, `"first"`)

	secondOutput, err := second.Run(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	requireJSONOutput(t, firstOutput, `"first"`)
	requireJSONOutput(t, secondOutput, `"second"`)
	firstOutput.Content[1] = 'X'
	requireJSONOutput(t, secondOutput, `"second"`)

	repeated, err := first.Run(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	requireJSONOutput(t, repeated, `"first"`)
	requireJSONOutput(t, firstOutput, `"Xirst"`)
	repeated.Content[1] = 'Y'
	requireJSONOutput(t, firstOutput, `"Xirst"`)
	requireJSONOutput(t, secondOutput, `"second"`)
}

func TestCompilerDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
		eof     bool
	}{
		{name: "invalid token", content: "RETURN )"},
		{name: "EOF", content: "RETURN", eof: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t)
			src := api.NewSource("invalid.fql", test.content)

			directPlan, directErr := h.hosted.Compile(h.ctx, src)
			if directPlan != nil {
				h.own(directPlan)
				t.Fatal("invalid source produced a hosted plan")
			}

			var expected diagnostics.Diagnostics
			if !errors.As(directErr, &expected) || len(expected) == 0 {
				t.Fatalf("hosted compile lacks portable diagnostics: %v", directErr)
			}

			plan, err := h.runtime.Compile(h.ctx, src)
			if plan != nil {
				h.own(plan)
				t.Fatal("invalid source produced a remote plan")
			}

			var remote *client.Error
			if !errors.As(err, &remote) || len(remote.Diagnostics) == 0 {
				t.Fatalf("remote compile lacks portable diagnostics: %v; hosted diagnostics: %#v", err, expected)
			}

			if remote.Category != failure.CategoryCompilation || status.Code(err) != codes.InvalidArgument {
				t.Fatalf("compile classification = category %v, code %v; want compilation/InvalidArgument", remote.Category, status.Code(err))
			}

			if !reflect.DeepEqual(remote.Diagnostics, expected) {
				t.Fatalf("diagnostics changed across Wire: got %#v, want %#v", remote.Diagnostics, expected)
			}

			diagnostic := remote.Diagnostics[0]
			if diagnostic.Source != src || diagnostic.Message == "" || len(diagnostic.Annotations) == 0 {
				t.Fatalf("incomplete compiler diagnostic: %+v", diagnostic)
			}

			location := diagnostic.Annotations[0].Range
			if location.SourceName != src.Name || location.Line < 1 || location.Column < 0 ||
				location.Span.Start < 0 || location.Span.End < location.Span.Start || location.Span.End > len(src.Content) {
				t.Fatalf("invalid diagnostic source range: %+v", location)
			}

			// Missing syntax at EOF is a zero-width insertion point. Wire must
			// preserve it without extending the span or normalizing the location.
			if test.eof && (location.Line != 1 || location.Column != 7 || location.Span.Start != 6 || location.Span.End != 6) {
				t.Fatalf("EOF diagnostic range = %+v; want 1:7 / [6,6)", location)
			}
		})
	}
}

func requireJSONOutput(t *testing.T, output *api.Output, want string) {
	t.Helper()

	if output == nil || output.ContentType != "application/json" || string(output.Content) != want {
		t.Fatalf("output = %+v, want application/json %s", output, want)
	}
}

func requireDetachedParams(t *testing.T, plan api.Plan) {
	t.Helper()

	params, err := plan.Params(t.Context())
	if err != nil || !reflect.DeepEqual(params, []string{"value"}) {
		t.Fatalf("Params = %v, %v; want [value]", params, err)
	}

	params[0] = "changed"

	again, err := plan.Params(t.Context())
	if err != nil || !reflect.DeepEqual(again, []string{"value"}) {
		t.Fatalf("Params after caller mutation = %v, %v; want [value]", again, err)
	}
}
