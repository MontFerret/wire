# Universal API integration contracts

These external-consumer tests exercise:

```text
api.Runtime caller → wire/client → protobuf and real gRPC over bufconn
                                → wire/server → hosted Universal API spies
```

The suite depends on the API version pinned in the root `go.mod` (currently
`v1.0.0-alpha.11`). It imports neither native Ferret nor Wire internal packages.
Existing component, conversion, low-level facade/protocol tests, and benchmarks
remain beside their owning packages. The former server-package Universal API
adapter, allocation, cancellation, and transport tests are consolidated here.

## Harness

`harness.New(t)` starts the public Wire server, opens a real gRPC connection, and
returns a canonical runtime through `h.Runtime()`. `h.RuntimeSpy().Recorder()`
provides copied call and lifecycle snapshots. `WithBehavior` configures immutable
runtime/plan/session/debugger behavior before startup; `WithRuntime` accepts a
custom API implementation; `WithServerOptions` applies public server limits.

The harness separates transport ownership, API doubles, option snapshots,
lifecycle recording, failure injection, and cancellation coordination. Recorded
object IDs identify hosted spy instances and parent relationships, never Wire
resource handles. Every hosted close attempt is counted, including duplicates.
Fake parents never close children on Wire's behalf.

`NewBlock`, `Await`, and recorder change notifications coordinate operations with
bounded waits. Hooks run outside recorder locks and must honor their context.
Use a fresh Block for each blocked invocation and await every test goroutine's
result. Do not use sleeps, polling, unbounded receives, or global goroutine counts.

`h.Faults()` wraps the real connection. Allocation gates hold replies only after
the real server has committed and the response has crossed gRPC. Semantic fault
operations support lost/malformed replies, releases that never arrive, lost
release acknowledgements, and watches ending after a real initial snapshot.
Only this fault infrastructure references generated method constants or mutates
received protobuf payloads. Tests never call generated clients or handlers.

`OpenRuntime` creates another logical client on the same physical connection.
`Shutdown` and `CloseTransport` model distinct lifetime failures. Cleanup removes
faults, closes logical clients, and asserts hosted resources finished closing
exactly once before server shutdown could hide a leak. It then shuts down the
server, closes owned transport and listener resources, and waits for serving to
finish. Expected retained cleanup errors must be registered
with `ExpectCleanupError`; connection-loss tests permit the corresponding
transport/closed-handle errors. Wire must never close the hosted runtime.

## Interface coverage

Every current interface method has end-to-end coverage. Names below are Go test
names; each linked file contains the complete scenario and assertions.

| Interface | Method | Contract test |
| --- | --- | --- |
| `api.Runtime` | `Run` | [TestRuntimeAndSessionOutputRoundTrip](runtime_test.go) |
| | `Compile` | [TestCompileRoundTrip](plan_test.go) |
| | `CompileDebug` | [TestCompileRoundTrip](plan_test.go), [TestDebuggerRoundTrip](debugger_test.go) |
| | `Close` | [TestRuntimeCloseBorrowsTransportAndHostedRuntime](runtime_test.go) |
| `api.Plan` | `Params` | [TestCompileRoundTrip](plan_test.go) |
| | `NewSession` | [TestReusablePlanAndDurableSessions](plan_test.go) |
| | `NewDebugSession` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Close` | [TestReusablePlanAndDurableSessions](plan_test.go), [TestRecursiveCloseReclaimsActiveDescendants](lifecycle_test.go) |
| `api.Session` | `Run` | [TestReusablePlanAndDurableSessions](plan_test.go), [TestSessionRejectsOverlapAndReopensAfterRelease](session_test.go) |
| | `Close` | [TestReusablePlanAndDurableSessions](plan_test.go), [TestRecursiveCloseReclaimsActiveDescendants](lifecycle_test.go) |
| `debugger.Session` | `Start` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Continue` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `StepOver` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `StepIn` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `StepOut` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Pause` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `SetBreakpoint` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `SetBreakpointAt` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `DeleteBreakpoint` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Breakpoints` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Frames` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Locals` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `FrameLocals` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Variables` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Evaluate` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `EvaluateFrame` | [TestDebuggerRoundTrip](debugger_test.go) |
| | `Close` | [TestDebuggerRoundTrip](debugger_test.go), [TestRecursiveCloseReclaimsActiveDescendants](lifecycle_test.go) |

Output has content and content type, without structured metadata. Diagnostics
have an open `Kind`, source, annotations, hint, and note, without a separate
severity field. Debugger frames use indices and function IDs. Convenience
operations may delegate to their indexed/default-binding equivalents; assertions
cover those documented semantic calls. Successful stopped events have nil
`Event.Error`; runtime-error stops preserve a public `failure.Failure`.

## Lifecycle and failure coverage

| Contract | Tests |
| --- | --- |
| Portable values, option overrides, output selection | [TestSessionOptionsRoundTrip](runtime_test.go) |
| Optimization presence, option ordering and failure before dispatch | [TestCompileRoundTrip, TestCompileOptionsApplyOnceBeforeDispatch](plan_test.go) |
| Plain and diagnostic-bearing compile/execution/debug failures; sanitization | [TestDiagnosticsAndFailureClassification](errors_test.go) |
| Invalid request/state, not found, expired deadline, remote cancellation, resource limits | [TestErrorFamilies](errors_test.go) |
| Runtime/session/Start/Continue/Evaluate cancellation and execution-slot reuse | [TestCancellationReachesHostedOperations](cancellation_test.go) |
| Detached compile allocation and hosted cancellation on logical shutdown | [TestCompileCancellationPreservesDetachedAllocation, TestLogicalShutdownCancelsHostedCompile](cancellation_test.go) |
| Cancellation before dispatch, after committed allocation, concurrent Close | [allocation_race_test.go](allocation_race_test.go) |
| Unknown plan/session/debugger/execution; nearest parent and escalation | [allocation_test.go](allocation_test.go) |
| Known-ID release delivery failure versus lost acknowledgement; sibling preservation | [release_test.go](release_test.go) |
| Execution completion racing cancellation with exactly one release | [TestSessionCompletionRacesCancellationWithoutDuplicateCleanup](session_test.go) |
| Recursive close, distinct concurrent plans/sessions, mixed execution/debug resources | [lifecycle_test.go](lifecycle_test.go) |
| Unavailable handshake, server shutdown or transport closure during active work | [TestUnavailableServer, TestConnectionLossReclaimsResources](connection_test.go) |
| Initial snapshots, fast completion, debugger transitions, premature EOF/transport errors | [runtime_test.go](runtime_test.go), [debugger_test.go](debugger_test.go), [TestWatchTerminationReturnsError](connection_test.go) |
| Debugger command failure distinct from a runtime-error stop | [TestDebuggerCommandFailure](connection_test.go) |
| Constructor panic, poisoned sessions/debuggers, cleanup panic and sanitization | [TestConstructorPanicPreservesParent, TestPanicContainmentAndResourcePoisoning](errors_test.go) |

Allocation and release policy is defined in [Client Handles](../../docs/client.md):
caller cancellation does not interrupt an in-flight bounded allocation, and an
eventual known handle is reclaimed before returning cancellation. Unknown IDs
require nearest-owner invalidation; failed release of a known ID does not.
Transport `ResourceExhausted` alone cannot distinguish a quota rejection from an
oversized committed response, so the existing conservative reclamation policy
applies. No test weakens this policy by treating the status alone as proof of
rejected creation.

Abrupt connection loss returns errors. Graceful server shutdown can deliver a
semantic debugger termination event before transport closure; the suite accepts
that terminal outcome. Runtime and normal-session execution can instead receive
`NotFound` if shutdown removes the logical connection or execution before the
client opens its watch. The suite accepts this only for server shutdown with a
public `ConnectionNotFound` or `ExecutionNotFound` category, and checks each joined
operation and cleanup error independently. Cancellation and exactly-once hosted
cleanup remain required. Premature watch closure must not look like successful
execution. Explicit debugger Close still owns cleanup after a watch-only failure.
No automatic reconnection or protocol changes are introduced.

## Running

```sh
go test ./test/integration/...
go test -race ./test/integration/...
go test -race ./test/integration -count=20 -shuffle=on
```

The root `make test` and `make test-race` include this suite on existing CI jobs.
Tests need no native runtime, external server, TCP port, or additional dependency.
