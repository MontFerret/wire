package server

import (
	"errors"
	"time"
)

type (
	// RunOption configures managed shutdown without changing constructor policy.
	// Run rejects nil functions and applies options in registration order.
	RunOption func(*runConfig) error

	runConfig struct {
		shutdownTimeout time.Duration
	}
)

// WithShutdownTimeout bounds Run's shutdown wait, starting when shutdown begins.
// The default is 30 seconds; an override must be positive. An explicit Shutdown
// deadline may shorten this budget. Timeout stops transport but does not imply
// hosted cleanup has settled; later Shutdown calls can observe its retained result.
func WithShutdownTimeout(timeout time.Duration) RunOption {
	return func(cfg *runConfig) error {
		if timeout <= 0 {
			return errors.New("shutdown timeout must be positive")
		}

		cfg.shutdownTimeout = timeout

		return nil
	}
}
