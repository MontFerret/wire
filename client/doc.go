// Package client implements the Universal Ferret API over a caller-owned gRPC
// connection. New returns api.Runtime; plans, sessions, output, options, and
// debugger values use github.com/MontFerret/api and its canonical subpackages.
//
// The construction context bounds the handshake, while the returned runtime
// owns the logical connection lifetime. Runtime.Run invokes the hosted runtime
// directly. Plans create durable sessions whose runs are sequential. Callers
// close resources explicitly; Close uses bounded detached cleanup and never
// closes the physical transport.
//
// Wire resource IDs, RPC clients, and watch streams remain private. Immediate
// remote failures expose Error, terminal failures use pkg/failure.Failure, and
// ErrClosed and ErrExecutionCancelled distinguish local closure and remote
// cancellation from the caller's context errors.
package client
