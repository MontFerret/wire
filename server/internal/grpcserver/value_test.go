package grpcserver

import (
	"math"
	"reflect"
	"strings"
	"testing"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestDecodeValueRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name  string
		value *wirev1.Value
		want  string
	}{
		{name: "nil", value: nil, want: "oneof"},
		{name: "missing oneof", value: &wirev1.Value{}, want: "oneof"},
		{name: "nil array", value: &wirev1.Value{Value: &wirev1.Value_ArrayValue{}}, want: "array value"},
		{name: "nil object", value: &wirev1.Value{Value: &wirev1.Value_ObjectValue{}}, want: "object value"},
		{name: "NaN", value: &wirev1.Value{Value: &wirev1.Value_FloatValue{FloatValue: math.NaN()}}, want: "finite"},
		{name: "positive infinity", value: &wirev1.Value{Value: &wirev1.Value_FloatValue{FloatValue: math.Inf(1)}}, want: "finite"},
		{name: "negative infinity", value: &wirev1.Value{Value: &wirev1.Value_FloatValue{FloatValue: math.Inf(-1)}}, want: "finite"},
		{name: "nested NaN", value: &wirev1.Value{Value: &wirev1.Value_ObjectValue{ObjectValue: &wirev1.ObjectValue{Fields: map[string]*wirev1.Value{
			"value": {Value: &wirev1.Value_ArrayValue{ArrayValue: &wirev1.ArrayValue{Values: []*wirev1.Value{{Value: &wirev1.Value_FloatValue{FloatValue: math.NaN()}}}}}},
		}}}}, want: "finite"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeValue(test.value, 0)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestDecodeValueRejectsExcessiveNesting(t *testing.T) {
	value := &wirev1.Value{Value: &wirev1.Value_StringValue{StringValue: "leaf"}}
	for range maxValueDepth {
		value = &wirev1.Value{Value: &wirev1.Value_ArrayValue{ArrayValue: &wirev1.ArrayValue{Values: []*wirev1.Value{value}}}}
	}
	_, err := decodeValue(value, 0)
	if err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeParametersBuildsTransportNeutralValues(t *testing.T) {
	parameters, err := decodeParameters(&wirev1.Parameters{Values: map[string]*wirev1.Value{
		"null":    {Value: &wirev1.Value_NullValue{}},
		"boolean": {Value: &wirev1.Value_BooleanValue{BooleanValue: true}},
		"integer": {Value: &wirev1.Value_IntegerValue{IntegerValue: math.MinInt64}},
		"float":   {Value: &wirev1.Value_FloatValue{FloatValue: 3.5}},
		"string":  {Value: &wirev1.Value_StringValue{StringValue: "wire"}},
		"binary":  {Value: &wirev1.Value_BinaryValue{BinaryValue: []byte{1, 2}}},
		"array": {Value: &wirev1.Value_ArrayValue{ArrayValue: &wirev1.ArrayValue{Values: []*wirev1.Value{
			{Value: &wirev1.Value_BooleanValue{BooleanValue: true}},
		}}}},
		"object": {Value: &wirev1.Value_ObjectValue{ObjectValue: &wirev1.ObjectValue{Fields: map[string]*wirev1.Value{
			"ok": {Value: &wirev1.Value_BooleanValue{BooleanValue: true}},
		}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	if value, ok := parameters["integer"].(int64); !ok || value != math.MinInt64 {
		t.Fatalf("unexpected integer: %#v", parameters["integer"])
	}

	if value, ok := parameters["null"]; !ok || value != nil {
		t.Fatalf("unexpected null: %#v", parameters["null"])
	}

	if !reflect.DeepEqual(parameters["array"], []any{true}) {
		t.Fatalf("unexpected array: %#v", parameters["array"])
	}

	if !reflect.DeepEqual(parameters["object"], map[string]any{"ok": true}) {
		t.Fatalf("unexpected object: %#v", parameters["object"])
	}
}
