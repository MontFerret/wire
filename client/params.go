package client

import (
	"fmt"
	"math"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const maxParameterDepth = 64

func encodeParameters(values map[string]any) (*wirev1.Parameters, error) {
	result := &wirev1.Parameters{Values: make(map[string]*wirev1.Value, len(values))}
	for name, value := range values {
		if name == "" {
			return nil, fmt.Errorf("parameter name must not be empty")
		}
		converted, err := encodeValue(value, 0)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", name, err)
		}
		result.Values[name] = converted
	}

	return result, nil
}

func encodeValue(value any, depth int) (*wirev1.Value, error) {
	if depth >= maxParameterDepth {
		return nil, fmt.Errorf("value nesting exceeds %d levels", maxParameterDepth)
	}

	switch value := value.(type) {
	case nil:
		return &wirev1.Value{Value: &wirev1.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
	case bool:
		return &wirev1.Value{Value: &wirev1.Value_BooleanValue{BooleanValue: value}}, nil
	case int:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case int8:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case int16:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case int32:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case int64:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: value}}, nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return nil, fmt.Errorf("unsigned integer exceeds int64")
		}

		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case uint8:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case uint16:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case uint32:
		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case uint64:
		if value > math.MaxInt64 {
			return nil, fmt.Errorf("unsigned integer exceeds int64")
		}

		return &wirev1.Value{Value: &wirev1.Value_IntegerValue{IntegerValue: int64(value)}}, nil
	case float32:
		converted := float64(value)
		if math.IsNaN(converted) || math.IsInf(converted, 0) {
			return nil, fmt.Errorf("floating-point value must be finite")
		}

		return &wirev1.Value{Value: &wirev1.Value_FloatValue{FloatValue: converted}}, nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, fmt.Errorf("floating-point value must be finite")
		}

		return &wirev1.Value{Value: &wirev1.Value_FloatValue{FloatValue: value}}, nil
	case string:
		return &wirev1.Value{Value: &wirev1.Value_StringValue{StringValue: value}}, nil
	case []byte:
		return &wirev1.Value{Value: &wirev1.Value_BinaryValue{BinaryValue: append([]byte(nil), value...)}}, nil
	case []any:
		items := make([]*wirev1.Value, len(value))
		for i, item := range value {
			converted, err := encodeValue(item, depth+1)
			if err != nil {
				return nil, fmt.Errorf("array item %d: %w", i, err)
			}
			items[i] = converted
		}

		return &wirev1.Value{Value: &wirev1.Value_ArrayValue{ArrayValue: &wirev1.ArrayValue{Values: items}}}, nil
	case map[string]any:
		fields := make(map[string]*wirev1.Value, len(value))
		for name, item := range value {
			if name == "" {
				return nil, fmt.Errorf("object key must not be empty")
			}
			converted, err := encodeValue(item, depth+1)
			if err != nil {
				return nil, fmt.Errorf("object field %q: %w", name, err)
			}
			fields[name] = converted
		}

		return &wirev1.Value{Value: &wirev1.Value_ObjectValue{ObjectValue: &wirev1.ObjectValue{Fields: fields}}}, nil
	default:
		return nil, fmt.Errorf("unsupported Go value type %T", value)
	}
}
