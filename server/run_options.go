package server

import (
	"time"

	gooptions "github.com/ziflex/go-options"
)

type (
	// RunOption configures managed shutdown without changing constructor policy.
	// Run applies every non-nil option in registration order and joins failures.
	// Nil functions are rejected before startup reservation or listening.
	// Factory validation failures expose gooptions.ValidationError through errors.As.
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
	return gooptions.New(func(cfg *runConfig, value time.Duration) {
		cfg.shutdownTimeout = value
	}).Value(timeout).Named("shutdown timeout").Validators(gooptions.Positive[time.Duration]()).Build()
}
