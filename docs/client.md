# Client Handles

The handwritten `client` package is a domain facade over the generated gRPC
clients. It owns one logical Wire connection while borrowing the caller's
`grpc.ClientConnInterface`.

## Resource model

Remote resources are exposed as opaque, typed handles:

```text
Client
└── Plan
    ├── Execution
    └── DebugSession
```

`Client` compiles plans. A `Plan` executes its compiled program and creates
debug sessions. Execution and debugger operations live on their respective
handles. The protocol still carries connection, plan, execution, and
debug-session IDs, but the facade retains and propagates them privately.
Callers cannot manually combine a handle with another client's connection.
Breakpoint IDs and debug value references remain visible because callers pass
them back to debugger operations; they do not expose connection or handle
ownership. A positive value reference is usable only while that debug session
remains in its current stopped state; zero and stale references are rejected.

Debugger inspection uses the canonical Unified API types directly:

```go
SetBreakpoint(context.Context, source.Location) (debugger.Breakpoint, error)
DeleteBreakpoint(context.Context, debugger.BreakpointID) error
Frames(context.Context) ([]debugger.Frame, error)
FrameLocals(context.Context, int) ([]debugger.Variable, error)
Variables(context.Context, debugger.ValueReference) ([]debugger.Variable, error)
EvaluateFrame(context.Context, int, string) (debugger.Value, error)
StepOver(context.Context) error
StepIn(context.Context) error
StepOut(context.Context) error
```

The slice index returned by `Frames` is the index accepted by `FrameLocals`
and `EvaluateFrame`; Wire does not expose or transmit a second frame-identity
type. Breakpoints preserve requested and resolved locations, spans, point and
function IDs, binding mode, and bound state. Frames preserve their function ID,
variables preserve mutable and parameter flags, and debug snapshots preserve
depth, stop reason, hit breakpoint IDs, output, and failure.
`source.Location.SourceName` is a semantic source identifier and is never
interpreted as a filesystem path by the client.

The protocol accepts explicit breakpoint binding options. The temporary client
facade continues to call `SetBreakpoint` with no explicit option, so the server
uses the Unified API default binding behavior. Exposing the option through a
redesigned client API is deferred.

Compilation and one-shot execution accept `api.Source` directly. Its `Name`
and `Content` map to the protocol `Source` message.

```go
src := api.NewSource("query.fql", "RETURN @input")
plan, err := wireClient.Compile(ctx, src, client.CompileOptions{})
if err != nil {
	return err
}
defer func() {
	if closeErr := plan.Close(context.Background()); closeErr != nil {
		log.Printf("close plan: %v", closeErr)
	}
}()

execution, err := plan.Execute(ctx, parameters, client.ExecuteOptions{})
if err != nil {
	return err
}
defer func() {
	if closeErr := execution.Close(context.Background()); closeErr != nil {
		log.Printf("close execution: %v", closeErr)
	}
}()

output, err := execution.Wait(ctx)
```

Plan metadata is immutable. `Parameters` returns a defensive copy.
`CompileOptions.Debuggable` temporarily chooses the protocol's `CompileDebug`
operation instead of `Compile`; the `Plan` retains that choice locally so
`Debuggable` remains compatible without adding debug capability to the remote
Plan resource.

The metadata facade maps `APIIdentity` and `WireVersion` from the handshake's
protocol name and version and maps optional host runtime identity directly to
`*execution.Identity` from `pkg/execution`.
Legacy Ferret-version and capability fields remain empty rather than
fabricating values not carried by the protocol.

## Convenience execution

For a one-shot program, `Client.Run` composes compile, execute, wait, and
ordered cleanup while preserving the Unified API encoded output boundary:

```go
output, err := wireClient.Run(
	ctx,
	api.NewSource("query.fql", "RETURN @input"),
	client.Parameters{"input": "hello"},
	client.RunOptions{Execute: client.ExecuteOptions{OutputContentType: "application/json"}},
)
```

A caller that owns a reusable plan can run it repeatedly without surrendering
plan ownership:

```go
src := api.NewSource("query.fql", "RETURN @input")
plan, err := wireClient.Compile(ctx, src, client.CompileOptions{})
if err != nil {
	return err
}
defer func() {
	if closeErr := plan.Close(context.Background()); closeErr != nil {
		log.Printf("close plan: %v", closeErr)
	}
}()

output, err := plan.Run(ctx, parameters, client.ExecuteOptions{})
```

The ownership boundary is explicit:

| Operation | Creates | Releases |
| --- | --- | --- |
| `Client.Run` | Plan and Execution | Plan and Execution |
| `Plan.Run` | Execution | Execution only |
| `Execution.Wait` | Nothing | Nothing |

`Execution.Wait` opens a fresh watch, ignores non-terminal snapshots, and
returns when the execution completes, fails, or is remotely cancelled. The
method, along with `Client.Run` and `Plan.Run`, returns `api.Output`. Failed
terminal snapshots return `*failure.Failure`; remote cancellation returns
`client.ErrExecutionCancelled`. Cancellation of the caller's waiting context
instead returns that context's error.

Convenience cleanup is synchronous and uses a cancellation-detached context
with a fresh 30-second deadline for each release, so resources created by
`Run` are still released after the request context is cancelled without
allowing a stalled cleanup call to block forever. Execution and cleanup errors
are joined rather than replacing one another.

## Snapshots and events

Handles represent identity, ownership, and operations; they are not mutable
state snapshots. Execution state crosses the facade as `execution.Event` and
`execution.Snapshot` from `pkg/execution`; debug state crosses as `debugger.Event`
and `debugger.Snapshot` from Wire's `pkg/debugger`. These snapshots preserve
Unified API `api.Output` and use API `debugger.Reason`, `source.Range`, and
`debugger.BreakpointID` values without exposing generated protobuf messages.
The client allocates fresh output bytes, diagnostics, ranges, and breakpoint-ID
slices while decoding each protobuf response.

Execution and debugger command methods return errors only. Their state changes
are observed through `Execution.Watch` or `DebugSession.Watch`. A watch sends
the server's latest published event first, then ordered changes through one
terminal event. A newly created debug session replays sequence 1 with
`debugger.EventCreated` and a created snapshot; the client does not need a Get
RPC.

`execution.Event` contains a sequence and execution snapshot.
`execution.State.Terminal` identifies completed, failed, and cancelled states.
Debug events retain a separate `debugger.EventKind` because starting and
continuing are distinct transitions that both publish a running snapshot.
`debugger.State.Terminal` centralizes completed, failed, and terminated state
handling.

Watch streams are tied to both the operation context and the logical Client
lifecycle. A watch opened before resource closure remains able to receive the
server's terminal event. New watches are rejected after the handle or an
ancestor starts closing.

## Closing resources

`Plan`, `Execution`, and `DebugSession` expose `Close(context.Context) error`.
Close maps to the corresponding protocol release operation; it is never driven
by finalizers or garbage collection.

The first Close commits teardown exactly once. Concurrent and repeated callers
wait for the same retained release result. A waiter's context can expire
without cancelling the committed cleanup, and a later call can still observe
the retained result. Release failures are retained rather than hidden or
retried. Once close begins, the handle rejects new operations with an error
matching `client.ErrClosed`.

Closing a Client or Plan owns cleanup of its descendants. Descendant operations
are rejected as soon as ancestor closure begins. A descendant first closed
after ancestor cleanup begins observes the ancestor's retained result rather
than issuing a duplicate release. Close children before parents when reporting
each resource's direct release result matters; normal `defer` ordering provides
this naturally.

`DebugSession.Stop` and `DebugSession.Close` are intentionally distinct. Stop
terminates debugger execution without releasing the remote ID; Close commits
termination and resource release.

## Errors

Immediate Wire failures are exposed as `*client.Error` with a
`failure.Category`, sanitized gRPC message, and canonical
`diagnostics.Diagnostics` when the runtime returned that typed collection.
Terminal `*failure.Failure` values carry the same canonical diagnostics. Wire
never parses error strings to construct them. Invalid requests, cancellation,
deadlines, unavailable transports, and resource exhaustion use their native
gRPC codes without a duplicate Wire category; `client.Error.Category` is zero
unless an `ErrorDetail` was transmitted. The error unwraps its transport cause,
so callers can use `status.Code(err)` without making transport codes part of
the semantic category. Protocol resource identifiers remain private.

Terminal execution and debug failures use `*failure.Failure`, while local
lifecycle and waiting conditions remain distinguishable through
`client.ErrClosed`, `client.ErrExecutionCancelled`, and context errors.
Convenience APIs join operation and cleanup errors so `errors.Is` and
`errors.As` continue to find each component.

## Facade responsibilities

The client package is limited to:

- logical Connect lifecycle;
- typed plan, execution, debugger, and event operations;
- private connection and resource-ID propagation;
- explicit parameter conversion;
- transport-neutral snapshots with defensive conversion and structured error mapping;
- hiding protobuf and gRPC ceremony without hiding protocol concepts.

Parameter conversion deliberately accepts only the portable Wire subset:
null, booleans, signed integers, finite doubles, strings, bytes, `[]any`, and
`map[string]any`. Exact signed `int64` and floating-point values remain distinct;
NaN and infinities are rejected even when nested. Duration, datetime, regexp,
and custom values are rejected locally. A broad client redesign, including a
smaller metadata surface and explicit compile and breakpoint options, is
deferred.

Closing the Client never closes the caller-owned gRPC connection. The facade
does not construct runtimes or transports. Its convenience execution methods
compose the same handles and watches; they do not duplicate runtime
semantics.
