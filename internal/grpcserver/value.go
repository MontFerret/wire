package grpcserver

import (
	"fmt"
	"math"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const maxValueDepth = 64

func decodeParameters(input *wirev1.Parameters) (map[string]any, error) {
	result := make(map[string]any)
	if input == nil {
		return result, nil
	}

	for name, inputValue := range input.GetValues() {
		if name == "" {
			return nil, fmt.Errorf("parameter name must not be empty")
		}

		value, err := decodeValue(inputValue, 0)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", name, err)
		}

		result[name] = value
	}

	return result, nil
}

func decodeValue(input *wirev1.Value, depth int) (any, error) {
	if input == nil || input.Value == nil {
		return nil, fmt.Errorf("value oneof is required")
	}

	if depth >= maxValueDepth {
		return nil, fmt.Errorf("value nesting exceeds %d levels", maxValueDepth)
	}

	switch value := input.Value.(type) {
	case *wirev1.Value_NullValue:
		if value.NullValue != structpb.NullValue_NULL_VALUE {
			return nil, fmt.Errorf("invalid null value")
		}

		return nil, nil
	case *wirev1.Value_BooleanValue:
		return value.BooleanValue, nil
	case *wirev1.Value_IntegerValue:
		return value.IntegerValue, nil
	case *wirev1.Value_FloatValue:
		if math.IsNaN(value.FloatValue) || math.IsInf(value.FloatValue, 0) {
			return nil, fmt.Errorf("floating-point value must be finite")
		}

		return value.FloatValue, nil
	case *wirev1.Value_StringValue:
		return value.StringValue, nil
	case *wirev1.Value_BinaryValue:
		return append([]byte(nil), value.BinaryValue...), nil
	case *wirev1.Value_ArrayValue:
		if value.ArrayValue == nil {
			return nil, fmt.Errorf("array value is required")
		}
		items := make([]any, len(value.ArrayValue.GetValues()))
		for i, item := range value.ArrayValue.GetValues() {
			converted, err := decodeValue(item, depth+1)
			if err != nil {
				return nil, fmt.Errorf("array item %d: %w", i, err)
			}
			items[i] = converted
		}

		return items, nil
	case *wirev1.Value_ObjectValue:
		if value.ObjectValue == nil {
			return nil, fmt.Errorf("object value is required")
		}

		fields := make(map[string]any, len(value.ObjectValue.GetFields()))
		for name, field := range value.ObjectValue.GetFields() {
			if name == "" {
				return nil, fmt.Errorf("object key must not be empty")
			}

			converted, err := decodeValue(field, depth+1)
			if err != nil {
				return nil, fmt.Errorf("object field %q: %w", name, err)
			}

			fields[name] = converted
		}

		return fields, nil
	default:
		return nil, fmt.Errorf("unsupported value variant")
	}
}
