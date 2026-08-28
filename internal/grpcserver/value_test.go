package grpcserver

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

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

func TestDecodeParametersBuildsTransportNeutralValues(t *testing.T) {
	parameters, err := decodeParameters(&wirev1.Parameters{Values: map[string]*wirev1.Value{
		"none":     {Value: &wirev1.Value_NoneValue{}},
		"boolean":  {Value: &wirev1.Value_BooleanValue{BooleanValue: true}},
		"integer":  {Value: &wirev1.Value_IntegerValue{IntegerValue: 42}},
		"float":    {Value: &wirev1.Value_FloatValue{FloatValue: 3.5}},
		"string":   {Value: &wirev1.Value_StringValue{StringValue: "wire"}},
		"binary":   {Value: &wirev1.Value_BinaryValue{BinaryValue: []byte{1, 2}}},
		"duration": {Value: &wirev1.Value_DurationNanos{DurationNanos: int64(5 * time.Second)}},
		"datetime": {Value: &wirev1.Value_DatetimeValue{DatetimeValue: "2026-08-28T12:34:56Z"}},
		"regexp":   {Value: &wirev1.Value_RegexpValue{RegexpValue: "^wire$"}},
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

	if value, ok := parameters["integer"].(int64); !ok || value != 42 {
		t.Fatalf("unexpected integer: %#v", parameters["integer"])
	}

	if value, ok := parameters["none"]; !ok || value != nil {
		t.Fatalf("unexpected none: %#v", parameters["none"])
	}

	if value, ok := parameters["duration"].(time.Duration); !ok || value != 5*time.Second {
		t.Fatalf("unexpected duration: %#v", parameters["duration"])
	}

	if value, ok := parameters["datetime"].(time.Time); !ok || value.Format(time.RFC3339Nano) != "2026-08-28T12:34:56Z" {
		t.Fatalf("unexpected datetime: %#v", parameters["datetime"])
	}

	if value, ok := parameters["regexp"].(*regexp.Regexp); !ok || value.String() != "^wire$" {
		t.Fatalf("unexpected regexp: %#v", parameters["regexp"])
	}

	if !reflect.DeepEqual(parameters["array"], []any{true}) {
		t.Fatalf("unexpected array: %#v", parameters["array"])
	}

	if !reflect.DeepEqual(parameters["object"], map[string]any{"ok": true}) {
		t.Fatalf("unexpected object: %#v", parameters["object"])
	}
}
