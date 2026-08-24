package grpcserver

import (
	"fmt"
	"time"

	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const maxValueDepth = 64

func decodeParameters(input *wirev1.Parameters) (ferretruntime.Params, error) {
	result := make(ferretruntime.Params)
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

func decodeValue(input *wirev1.Value, depth int) (ferretruntime.Value, error) {
	if input == nil || input.Value == nil {
		return nil, fmt.Errorf("value oneof is required")
	}

	if depth >= maxValueDepth {
		return nil, fmt.Errorf("value nesting exceeds %d levels", maxValueDepth)
	}

	switch value := input.Value.(type) {
	case *wirev1.Value_NoneValue:
		if value.NoneValue != structpb.NullValue_NULL_VALUE {
			return nil, fmt.Errorf("invalid none value")
		}

		return ferretruntime.None, nil
	case *wirev1.Value_BooleanValue:
		return ferretruntime.Boolean(value.BooleanValue), nil
	case *wirev1.Value_IntegerValue:
		return ferretruntime.Int(value.IntegerValue), nil
	case *wirev1.Value_FloatValue:
		return ferretruntime.Float(value.FloatValue), nil
	case *wirev1.Value_StringValue:
		return ferretruntime.String(value.StringValue), nil
	case *wirev1.Value_BinaryValue:
		return ferretruntime.NewBinary(append([]byte(nil), value.BinaryValue...)), nil
	case *wirev1.Value_DurationNanos:
		return ferretruntime.NewDuration(time.Duration(value.DurationNanos)), nil
	case *wirev1.Value_DatetimeValue:
		parsed, err := time.Parse(time.RFC3339Nano, value.DatetimeValue)
		if err != nil {
			return nil, fmt.Errorf("invalid RFC3339Nano datetime: %w", err)
		}

		return ferretruntime.NewDateTime(parsed), nil
	case *wirev1.Value_RegexpValue:
		parsed, err := ferretruntime.NewRegexp(value.RegexpValue)
		if err != nil {
			return nil, fmt.Errorf("invalid regexp: %w", err)
		}

		return parsed, nil
	case *wirev1.Value_ArrayValue:
		if value.ArrayValue == nil {
			return nil, fmt.Errorf("array value is required")
		}
		items := make([]ferretruntime.Value, len(value.ArrayValue.GetValues()))
		for i, item := range value.ArrayValue.GetValues() {
			converted, err := decodeValue(item, depth+1)
			if err != nil {
				return nil, fmt.Errorf("array item %d: %w", i, err)
			}
			items[i] = converted
		}

		return ferretruntime.NewArrayOf(items), nil
	case *wirev1.Value_ObjectValue:
		if value.ObjectValue == nil {
			return nil, fmt.Errorf("object value is required")
		}
		fields := make(map[string]ferretruntime.Value, len(value.ObjectValue.GetFields()))
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

		return ferretruntime.NewObjectWith(fields), nil
	default:
		return nil, fmt.Errorf("unsupported value variant")
	}
}
