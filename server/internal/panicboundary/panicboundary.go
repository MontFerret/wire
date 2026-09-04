// Package panicboundary contains panics raised by externally supplied
// implementations at an explicit call boundary.
package panicboundary

import "runtime/debug"

// Error records a panic value and the stack captured while recovering it.
// The panic value is deliberately not exposed through error unwrapping because
// a panic is an implementation failure, not an ordinary returned error.
type Error struct {
	Value any
	Stack []byte
}

func (e *Error) Error() string {
	return "external implementation panicked"
}

// Do invokes fn and converts a panic into an Error.
func Do(fn func() error) (err error) {
	completed := false
	defer func() {
		if completed {
			return
		}

		err = &Error{Value: recover(), Stack: debug.Stack()}
	}()

	err = fn()
	completed = true

	return err
}

// Call invokes fn and converts a panic into an Error and the zero value of T.
func Call[T any](fn func() (T, error)) (result T, err error) {
	completed := false
	defer func() {
		if completed {
			return
		}

		var zero T
		result = zero
		err = &Error{Value: recover(), Stack: debug.Stack()}
	}()

	result, err = fn()
	completed = true

	return result, err
}
