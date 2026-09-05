# Client Handles

The handwritten `client` package is a domain facade over the generated gRPC
clients. It owns one logical Wire connection while borrowing the caller's
`grpc.ClientConnInterface`.

## Resource model

Remote resources are exposed as opaque, typed handles:

```text
Runtime (= api.Runtime; private Wire implementation)
└── private Universal API adapters
    ├── api.Plan
    │   ├── Session (= api.Session)
    │   └── debugger.Session
    └── temporary Execution handles

Client (lower-level Wire facade)
└── Plan
    ├── Execution
    └── DebugSession
```

`NewRuntime(ctx, conn)` is the Universal API-first entry point. It returns
`Runtime`, an alias of `api.Runtime`, backed by a private `remoteRuntime` that
owns one logical `Client`. The returned Plan, normal Session, and debugger
adapters also remain private behind their Universal API interfaces.
The runtime adapter's `Run` uses `RuntimeService.Run` to call the hosted
`api.Runtime.Run` directly. A normal Session is created remotely once and each sequential `Run` uses a hidden
Execution that is watched and released before returning. Closing any adapter
uses the 30-second detached cleanup bound; closing Runtime never closes `conn`.

The client re-exports three canonical types for ordinary remote-runtime use:

| Alias | Canonical contract | Ordinary use |
| --- | --- | --- |
| `client.Runtime` | `api.Runtime` | Store or pass the runtime returned by `NewRuntime`. |
| `client.Session` | `api.Session` | Store or pass reusable normal sessions created by a plan. |
| `client.Output` | `api.Output`, defined in `api/result` | Consume encoded execution results. |

These are true Go aliases: ownership and type identity stay in
`github.com/MontFerret/api` and its canonical subpackages. Source construction,
options, diagnostics, debugger inspection values, and shared Wire snapshots
continue to use their owning packages. The existing `client.Plan` and
`client.DebugSession` names denote lower-level Wire handles; the Universal API
workflow returns `api.Plan` and `api/debugger.Session` instead.

`client.Runtime` was previously an exported concrete adapter. Explicit
`*client.Runtime` declarations must become `client.Runtime`; inferred
`remote, err := client.NewRuntime(ctx, conn)` calls remain unchanged. Constructor
failures return a nil interface and the existing error.

`Client` compiles plans. A `Plan` executes its compiled program and creates
debug sessions. Execution and debugger operations live on their respective
handles. The protocol still carries connection, plan, normal-session,
execution, and debug-session IDs, but the facade retains and propagates them privately.
Callers cannot manually combine a handle with another client's connection.
Breakpoint IDs and debug value references remain visible because callers pass
them back to debugger operations; they do not expose connection or handle
ownership. A positive value reference is usable only while that debug session
remains in its current stopped state; zero and stale references are rejected.

Debugger inspection uses the canonical Unified API types directly:

```go
SetBreakpoint(context.Context, source.Location) (debugger.Breakpoint, error)
SetBreakpointAt(context.Context, source.Location, debugger.BreakpointOptions) (debugger.Breakpoint, error)
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

The protocol and lower-level facade accept explicit breakpoint binding options
through `SetBreakpointAt`. `SetBreakpoint` remains the default-binding
convenience.

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
`CompileOptions.Debuggable` chooses the protocol's `CompileDebug` operation
instead of `Compile`. `CompileOptions.PlanOptions` accepts the same
`api.PlanOption` callbacks as the Universal API adapter:

```go
options := client.CompileOptions{
    PlanOptions: []api.PlanOption{api.WithOptimizationLevel(api.OptimizationNone)},
}
```

Omitting the optimization option preserves the hosted runtime default; supplying
it transports the explicit level, including `api.OptimizationNone`. Presence
tracking stays private. Both entry points apply non-nil callbacks exactly once,
in order, before dispatch. The last valid setting wins; callback errors are
joined and prevent dispatch, as does caller cancellation before or after option
application.

The metadata facade maps `APIIdentity` and `WireVersion` from the handshake's
protocol name and version and maps optional host runtime identity directly to
`*execution.Identity` from `pkg/execution`.
Legacy Ferret-version and capability fields remain empty rather than
fabricating values not carried by the protocol.

### Allocation and cancellation

The Universal API adapter checks cancellation before sending an allocation
request, including after option callbacks. Only the acquisition RPC is detached
from caller cancellation, with an internal 30-second deadline. If its resource
handle arrives after cancellation, the adapter releases that handle before
returning the caller's cancellation. Execution waiting uses the original caller
context; releasing the temporary Execution also cancels unfinished work.

A lost reply or a reply without a usable resource ID cannot be reclaimed by ID.
The adapter immediately closes the narrowest known owner: a Plan for an unknown
normal or debug Session, a Session for its unknown Execution, or the logical
Runtime for a root Plan or direct Runtime Execution. Successful narrow cleanup
preserves resources outside that subtree. Confirmed creation rejections and
local validation errors do not trigger this invalidation.

Each automatic release attempt has a fresh 30-second bound. For an unknown
child, a failed or expired parent release advances to the next known ancestor:
Session, then Plan, then logical Runtime. Successful reclamation stops that
escalation. If connection release fails, cancelling its Connect stream supplies
the final lifetime signal.

A handle with a known ID follows ordinary release policy, including when it
arrives after caller cancellation or belongs to a one-shot invocation. A failed
release is retained and returned without automatically invalidating its Session,
Plan, or Runtime. If release never reached the server, the hosted child can
remain until the caller explicitly closes its ancestor; a durable Session can
therefore remain busy. If only the release acknowledgement was lost after
server cleanup, the same durable Session can run again.

Operation and cleanup errors remain joined. The caller-owned physical transport
and other logical clients on it remain open. These bounds limit client waiting;
the hosted implementation must still honor its cancellation and Close contracts.


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
method, along with `Client.Run` and `Plan.Run`, returns `Output`, the alias of
`api.Output`. Failed terminal snapshots return `*failure.Failure`; remote cancellation returns
`client.ErrExecutionCancelled`. Cancellation of the caller's waiting context
instead returns that context's error.

Convenience cleanup is synchronous and uses a cancellation-detached context
with a fresh 30-second deadline for each release, so resources created by
`Run` are still released after the request context is cancelled without
allowing a stalled cleanup call to block forever. Execution and cleanup errors
are joined rather than replacing one another.

Universal API resource-allocation RPCs use the same bounded detached context.
This prevents a cancellation race from discarding the only opaque handle after
the server has published a resource. The original caller context is checked
after allocation; a resource that raced cancellation is cancelled and released
before the adapter returns the caller-visible cancellation.

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

The lower-level `Plan`, `Execution`, and `DebugSession` expose
`Close(context.Context) error`.
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
- a complete remote `api.Runtime` adapter;
- typed plan, execution, debugger, and event operations;
- private connection and resource-ID propagation;
- explicit parameter conversion;
- transport-neutral snapshots with defensive conversion and structured error mapping;
- hiding protobuf and gRPC ceremony without hiding protocol concepts.

Parameter conversion deliberately accepts only the portable Wire subset:
null, booleans, signed integers, finite doubles, strings, bytes, `[]any`, and
`map[string]any`. Exact signed `int64` and floating-point values remain distinct;
NaN and infinities are rejected even when nested. Duration, datetime, regexp,
and custom values are rejected locally. Further lower-level client redesign is
separate from the Universal API adapter.

Closing the Client never closes the caller-owned gRPC connection. The facade
does not construct runtimes or transports. Its convenience execution methods
compose the same handles and watches; they do not duplicate runtime
semantics.
