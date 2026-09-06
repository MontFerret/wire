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
	values, err := encodeParameters(map[string]any{
		"null":    nil,
		"boolean": true,
		"integer": int32(7),
		"float":   3.5,
		"string":  "wire",
		"binary":  []byte{1, 2},
		"array":   []any{1, "two"},
		"object":  map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := values.Values["null"].Value.(*wirev1.Value_NullValue); !ok {
		t.Fatalf("null was not encoded explicitly: %#v", values.Values["null"])
	}

	if _, ok := values.Values["object"].Value.(*wirev1.Value_ObjectValue); !ok {
		t.Fatalf("object was not encoded explicitly: %#v", values.Values["object"])
	}
}

func TestEncodeParametersRejectsUnsupportedAndOutOfRangeValues(t *testing.T) {
	for _, values := range []map[string]any{
		{"too_large": uint64(math.MaxInt64) + 1},
		{"nan64": math.NaN()},
		{"positive_infinity64": math.Inf(1)},
		{"negative_infinity64": math.Inf(-1)},
		{"positive_infinity32": float32(math.Inf(1))},
		{"nested_nan": map[string]any{"value": []any{math.NaN()}}},
		{"unsupported": []string{"implicit reflection is not supported"}},
		{"duration": 5 * time.Second},
		{"datetime": time.Now()},
		{"regexp": regexp.MustCompile("^wire$")},
		{"regexp": (*regexp.Regexp)(nil)},
	} {
		_, err := encodeParameters(values)
		if err == nil {
			t.Fatalf("expected conversion failure for %#v", values)
		}
	}

	boundaries, err := encodeParameters(map[string]any{"minimum": int64(math.MinInt64), "maximum": int64(math.MaxInt64)})
	if err != nil {
		t.Fatal(err)
	}

	if boundaries.GetValues()["minimum"].GetIntegerValue() != math.MinInt64 ||
		boundaries.GetValues()["maximum"].GetIntegerValue() != math.MaxInt64 {
		t.Fatalf("signed int64 boundaries changed: %#v", boundaries.GetValues())
	}

	nested := any("leaf")
	for range maxParameterDepth {
		nested = []any{nested}
	}

	_, err = encodeParameters(map[string]any{"nested": nested})
	if err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("unexpected nesting error: %v", err)
	}
}
