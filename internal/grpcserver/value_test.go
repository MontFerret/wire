package grpcserver

import (
	"strings"
	"testing"

	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
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
		{name: "invalid datetime", value: &wirev1.Value{Value: &wirev1.Value_DatetimeValue{DatetimeValue: "tomorrow"}}, want: "RFC3339Nano"},
		{name: "invalid regexp", value: &wirev1.Value{Value: &wirev1.Value_RegexpValue{RegexpValue: "["}}, want: "regexp"},
		{name: "nil array", value: &wirev1.Value{Value: &wirev1.Value_ArrayValue{}}, want: "array value"},
		{name: "nil object", value: &wirev1.Value{Value: &wirev1.Value_ObjectValue{}}, want: "object value"},
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

func TestDecodeParametersBuildsExplicitFerretValues(t *testing.T) {
	parameters, err := decodeParameters(&wirev1.Parameters{Values: map[string]*wirev1.Value{
		"integer": {Value: &wirev1.Value_IntegerValue{IntegerValue: 42}},
		"array": {Value: &wirev1.Value_ArrayValue{ArrayValue: &wirev1.ArrayValue{Values: []*wirev1.Value{
			{Value: &wirev1.Value_BooleanValue{BooleanValue: true}},
		}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := parameters["integer"].(ferretruntime.Int); !ok || value != 42 {
		t.Fatalf("unexpected integer: %#v", parameters["integer"])
	}
	if _, ok := parameters["array"].(*ferretruntime.Array); !ok {
		t.Fatalf("unexpected array: %#v", parameters["array"])
	}
}
