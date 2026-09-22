package client

import "testing"

func TestSignedFunctionIDOverridesLegacyRepresentation(t *testing.T) {
	for _, value := range []int64{-1, 0, 17} {
		actual, err := convertFunctionID(^uint64(0), &value)
		if err != nil || int64(actual) != value {
			t.Fatalf("signed function %d: %d %v", value, actual, err)
		}
	}

	invalid := int64(-2)
	if _, err := convertFunctionID(0, &invalid); err == nil {
		t.Fatal("invalid negative function ID accepted")
	}

	if actual, err := convertFunctionID(17, nil); err != nil || actual != 17 {
		t.Fatalf("legacy function ID: %d %v", actual, err)
	}
}
