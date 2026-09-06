package panicboundary

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCallReturnsNormalResult(t *testing.T) {
	value, err := Call(func() (int, error) {
		return 42, nil
	})
	if err != nil || value != 42 {
		t.Fatalf("normal result = %d, %v", value, err)
	}
}

func TestCallReturnsNormalErrorUnchanged(t *testing.T) {
	sentinel := errors.New("sentinel")

	value, err := Call(func() (int, error) {
		return 0, sentinel
	})
	if value != 0 {
		t.Fatalf("value = %d, want zero", value)
	}

	if err != sentinel { //nolint:errorlint // Verify exact preservation of returned errors and recovered panic values.
		t.Fatalf("error identity was not retained: %v", err)
	}
}

func TestCallConvertsPanicToTypedError(t *testing.T) {
	panicValue := &struct{ secret string }{secret: "diagnostic"}

	value, err := Call(func() (int, error) {
		panic(panicValue)
	})
	if value != 0 {
		t.Fatalf("panic result = %d, want zero", value)
	}

	var panicErr *Error
	if !errors.As(err, &panicErr) {
		t.Fatalf("panic did not produce *Error: %T", err)
	}

	if panicErr.Value != panicValue {
		t.Fatalf("panic value = %#v, want %#v", panicErr.Value, panicValue)
	}

	if got := panicErr.Error(); got != "external implementation panicked" {
		t.Fatalf("panic error message = %q", got)
	}

	if len(panicErr.Stack) == 0 || !strings.Contains(string(panicErr.Stack), "TestCallConvertsPanicToTypedError") {
		t.Fatalf("panic stack was not captured: %q", panicErr.Stack)
	}

	wrapped := fmt.Errorf("call runtime: %w", err)

	var wrappedPanicErr *Error
	if !errors.As(wrapped, &wrappedPanicErr) || wrappedPanicErr != panicErr {
		t.Fatalf("wrapped panic error was not discoverable: %v", wrapped)
	}
}

func TestDoReturnsNormalErrorUnchanged(t *testing.T) {
	sentinel := errors.New("sentinel")

	if err := Do(func() error { return nil }); err != nil {
		t.Fatalf("normal success returned error: %v", err)
	}

	if err := Do(func() error { return sentinel }); err != sentinel { //nolint:errorlint // Verify exact preservation of returned errors and recovered panic values.
		t.Fatalf("error identity was not retained: %v", err)
	}
}

func TestDoDoesNotUnwrapErrorValuedPanic(t *testing.T) {
	err := Do(func() error {
		panic(context.Canceled)
	})

	var panicErr *Error
	if !errors.As(err, &panicErr) {
		t.Fatalf("panic did not produce *Error: %T", err)
	}

	if panicErr.Value != context.Canceled { //nolint:errorlint // Verify exact preservation of returned errors and recovered panic values.
		t.Fatalf("panic value = %#v, want context.Canceled", panicErr.Value)
	}

	if len(panicErr.Stack) == 0 {
		t.Fatal("panic stack was not captured")
	}

	if errors.Is(err, context.Canceled) {
		t.Fatal("error-valued panic was exposed through errors.Is")
	}
}
