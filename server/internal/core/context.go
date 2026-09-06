package core

import "context"

// OperationContext preserves request values and deadlines while joining the
// resource lifetime. Call cancel to detach the lifetime cancellation callback.
func OperationContext(parent, lifetime context.Context) (context.Context, context.CancelFunc) {
	operation, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(lifetime, func() { cancel(context.Cause(lifetime)) })
	if lifetime.Err() != nil {
		cancel(context.Cause(lifetime))
	}

	return operation, func() {
		stop()
		cancel(context.Canceled)
	}
}
