// Package client implements the Unified Ferret API over gRPC. New creates and
// owns a channel with TLS by default; From borrows a caller-configured connection.
// Both return api.Runtime; plans, sessions, output, semantic options, and
// debugger values use github.com/MontFerret/api and its canonical subpackages.
//
// The construction context bounds the handshake, while the returned runtime
// owns the logical connection lifetime. Runtime.Run invokes the hosted runtime
// directly. Plans create durable sessions whose runs are sequential. Callers
// close resources explicitly. Close gates new root calls while admitted work and
// retained descendants keep their lifetimes. Final logical release uses bounded
// detached cleanup, then closes a New-owned channel. From never closes transport.
// Failed startup returns a nil runtime; rollback can outlast startup cancellation.
//
// Wire resource IDs, RPC clients, and watch streams remain private. Immediate
// remote failures expose Error, terminal failures use pkg/failure.Failure, and
// ErrClosed and ErrExecutionCancelled distinguish local closure and remote
// cancellation from the caller's context errors.
package client
