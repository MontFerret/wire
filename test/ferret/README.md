# Native Ferret compatibility

This focused ecosystem suite exercises the complete public boundary:

```text
native engine.New → ferret/uapi.Wrap(native, version) → api.Runtime → wire/server
→ protobuf and real gRPC over bufconn → wire/client.New → api.Runtime consumer
```

The nested module pins Ferret `v2.0.0-alpha.57` and Universal API
`v1.0.0-alpha.20`, matching Wire's UAPI dependency. It replaces only Wire with
the local checkout. Native Ferret is not a root-module dependency and is never
imported by Wire production packages. No workspace file or local Ferret checkout
is required.

The [hosted-spy suite](../integration/README.md) remains responsible for
exhaustive Wire contracts, fault injection, cancellation, quotas, and cleanup
counts. These tests instead verify that the real native implementation and
Wire's projection compose correctly. Both layers cross real serialization.

The harness supplies an opaque deterministic version to `uapi.Wrap`; the native
Version round trip compares direct and remote adapter results without hardcoding
a Ferret release string or deriving a version from Wire/build metadata.

## Coverage

| Test | Contract |
| --- | --- |
| `TestRuntimeVersionRoundTrip` | Exact native and remote version equality, including cached reads after Wire cleanup |
| `TestPlanParametersRoundTrip` | Normal/debug local and remote `Params(ctx)` equality, empty and ordered multi-parameter snapshots, detached storage |
| `TestRuntimeRun` | Real FQL, bind parameter, JSON output, and a filesystem-root override distinct from the engine default |
| `TestReusablePlanAndDurableSessions` | Detached parameter metadata, independent sessions, repeated session runs, and detached encoded output |
| `TestPlanClosePreservesSession` | Existing session executes after plan Close; new constructors fail while detached metadata survives |
| `TestRuntimeCloseDefersConnectionRelease` | Closed root rejects work; descendants still create/execute; the last child releases the logical connection slot |
| `TestWireShutdownBorrowsNativeRuntime` | Direct hosted execution still works after Wire and transport shutdown |
| `TestCompilerDiagnostics` | Invalid-token and EOF compiler diagnostics preserve their compilation classification, source, annotations, and ranges through Wire |
| `TestDebuggerRoundTrip` | Explicit unoptimized compilation, breakpoint replacement/binding/enumeration, entry and breakpoint stops, three-frame locals/evaluation, completion and output |

## Ownership and determinism

The harness owns a separately constructed native engine. The UAPI adapter and
Wire server borrow it; the client owns its logical connection and borrows the
test-owned gRPC transport. Tests register every returned plan/session for cleanup
and close descendants before runtimes and transports. Engine closure happens
last, after Wire shutdown and Serve completion. All meaningful cleanup errors
are checked, including partial setup failures.

Operation contexts and shutdown/channel waits have ten-second bounds. The
debugger Start context stays alive through completion. Package test commands
also have a two-minute timeout. Connection reclamation is observed with public
`MaxConnections=1` admission, without private IDs, polling, or sleeps.

`uapi.Wrap.Close` is a documented no-op: the native ownership test proves
continued usability, while the spy suite proves that Wire never calls hosted
`Runtime.Close`. Compiler assertions inspect `client.Error.Diagnostics`, the
existing portable error field, without depending on native error types.

Tests need no Chrome, external services, fixed ports, CLI binaries, or installed
ferretd. Dependency installation needs the usual Go module downloads; execution
uses only temporary directories and in-process gRPC.

## Compiler diagnostics

The diagnostic test compares complete portable diagnostics from direct hosted
compilation with those returned through Wire for both `RETURN )` and bare
`RETURN`. Both must retain the compilation-error category and gRPC
`InvalidArgument` status. The EOF case explicitly verifies line `1`, column `7`,
and the zero-width insertion span `[6,6)` supplied by Ferret. Wire
preserves those coordinates without repairing or normalizing hosted ranges.

## Running

From the repository root:

```sh
make test-ferret
make test-ferret-race
make check-ferret-tidy
```

Root `go test ./...` intentionally does not discover this nested module. These
targets enter it explicitly with `GOWORK=off`. Canonical `make fmt`,
`make check-fmt`, `make lint`, and `make vet` cover both modules with shared
configuration. CI runs the native suite and tidy check on Linux, macOS, and
Windows, and the race suite on Linux, alongside all existing Wire checks.
