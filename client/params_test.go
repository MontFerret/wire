package client

import (
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestEncodeParametersUsesExplicitWireVariants(t *testing.T) {
	when := time.Date(2026, 8, 23, 1, 2, 3, 4, time.FixedZone("test", -4*60*60))
	values, err := encodeParameters(map[string]any{
		"none":     nil,
		"boolean":  true,
		"integer":  int32(7),
		"float":    3.5,
		"string":   "wire",
		"binary":   []byte{1, 2},
		"duration": 5 * time.Second,
		"datetime": when,
		"regexp":   regexp.MustCompile("^wire$"),
		"array":    []any{1, "two"},
		"object":   map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := values.Values["duration"].Value.(*wirev1.Value_DurationNanos); !ok {
		t.Fatalf("duration was not encoded explicitly: %#v", values.Values["duration"])
	}

	if got := values.Values["datetime"].GetDatetimeValue(); got != when.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected datetime: %q", got)
	}

	if _, ok := values.Values["object"].Value.(*wirev1.Value_ObjectValue); !ok {
		t.Fatalf("object was not encoded explicitly: %#v", values.Values["object"])
	}
}

func TestEncodeParametersRejectsUnsupportedAndOutOfRangeValues(t *testing.T) {
	for _, values := range []map[string]any{
		{"too_large": uint64(math.MaxInt64) + 1},
		{"unsupported": []string{"implicit reflection is not supported"}},
		{"regexp": (*regexp.Regexp)(nil)},
	} {
		_, err := encodeParameters(values)
		if err == nil {
			t.Fatalf("expected conversion failure for %#v", values)
		}
	}

	nested := any("leaf")
	for range maxParameterDepth {
		nested = []any{nested}
	}
	_, err := encodeParameters(map[string]any{"nested": nested})
	if err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("unexpected nesting error: %v", err)
	}
}
