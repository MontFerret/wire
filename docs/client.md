# Client API

`client.New(ctx, target, options...)` creates a gRPC channel, completes and
validates the Wire handshake, and returns a remote `api.Runtime`.
`client.From(ctx, connection)` performs the same handshake over a caller-configured
`grpc.ClientConnInterface` and never takes ownership of that transport.
There is no second handwritten Wire resource API.

| Constructor | Logical Wire connection | gRPC channel |
| --- | --- | --- |
| `New(ctx, target, options...)` | Managed by Wire | Created and eventually closed by Wire |
| `From(ctx, connection)` | Managed by Wire | Always caller-owned |

Both require a non-nil, uncanceled startup context. `From` rejects nil and typed-nil
connections without requiring a transport `Close` method. Both return a genuinely
nil runtime interface on failure and retain decoded startup and rollback errors.
The target is required and passed unchanged to gRPC; only empty or whitespace-only
targets are rejected locally. Ordinary endpoints and resolver-qualified targets
use gRPC semantics. Each `New` creates its own independent channel.

### Transport options

`New` defaults to fresh TLS credentials with system trust roots and normal chain
and server-identity verification. There is no plaintext fallback. `WithInsecure`
explicitly selects plaintext, disabling encryption and peer authentication; it
does not mean TLS without certificate verification.

`WithTransportCredentials` replaces the default and passes custom TLS, mTLS, or
other credential policy through gRPC without modifying or owning the provider.
`WithPerRPCCredentials` installs providers on the channel for the handshake,
subsequent unary and streaming RPCs, and detached cleanup. Providers may refresh
values between calls; gRPC enforces `RequireTransportSecurity`.

Options run once in order. Nil options and nil/typed-nil credentials fail before
channel creation. Repeated transport credentials use the last valid value;
per-RPC providers append in registration order; repeated `WithInsecure` is
idempotent. Explicit insecure and custom transport options conflict in either
order. A later override cannot erase earlier validation errors. Reusing an
option does not accumulate configuration across runtimes. Provider implementations
must support concurrent calls; arbitrary provider state is not deep-copied.

`From` takes no transport options. Use it for custom dialers, interceptors,
message limits, or other specialized gRPC configuration. See the
[security examples](security.md) for connected TLS, mTLS, and authentication examples.

## Resource model

```text
api.Runtime
└── api.Plan
    ├── api.Session
    └── api/debugger.Session
```

All implementations remain private. Sources, options, output, diagnostics,
breakpoints, locations, frames, variables, reasons, and debugger events use
their canonical Universal API types directly. The constructor surface is `New`,
`From`, `Option`, `WithTransportCredentials`, `WithPerRPCCredentials`, and
`WithInsecure`; the remaining exports are `Error`, `ErrClosed`, and
`ErrExecutionCancelled`.

`Runtime.Run` invokes the hosted `api.Runtime.Run` directly, once per call.
`Compile` and `CompileDebug` create reusable plans through the corresponding
hosted methods. `Plan.Params(ctx) ([]string, error)` returns a defensive copy of
metadata captured before the compiled plan was published. A hosted metadata error or panic closes
the unpublished plan once and preserves its cleanup error. Each `NewSession`
creates one durable hosted session with the supplied semantic options;
sequential `Session.Run` calls reuse it. A concurrent run on that session is
rejected until the previous invocation's temporary execution has been released.
Distinct sessions and plans may execute concurrently.

Each runtime/session invocation privately acquires, watches, and releases an
execution. Output is `*api.Output`: its content type and encoded bytes are
copied without interpretation. Nil output differs from present empty output,
and available output survives execution or cleanup errors. A hosted `(nil, nil)`
result is invalid. No IDs, RPC handles, Wire execution snapshots, or protocol/host
identity metadata are exposed by the returned API interfaces. `Runtime.Version(ctx)` exposes only
the portable UAPI runtime version captured during Connect.

The Connect snapshot requires a present `runtime_version`, including when empty.
Its bytes are converted to `api.Version` unchanged, including invalid UTF-8.
Absent version metadata follows the invalid-Connect handshake path; host identity
is never substituted. `Runtime.Version(ctx)` and `Plan.Params(ctx)` reject nil
contexts and preserve `context.Canceled` and `context.DeadlineExceeded`. Both read
immutable snapshots without another RPC or lifecycle admission and remain available
after resource or transport cleanup. Parameter slices are caller-owned.

## Options and parameters

Use `api.WithOptimizationLevel` for plan compilation and `api.WithParam`,
`api.WithParams`, `api.WithFSRoot`, and `api.WithOutputContentType` for direct runs and session
creation. There are no Wire-specific semantic option structs. Omitted filesystem
roots and content types differ from explicitly empty strings. Wire preserves
both presence and value; the host owns path validation and codec availability.

Omitting optimization preserves the hosted default; an explicit
`api.OptimizationNone` transports the zero level. Non-nil callbacks run exactly
once in order. Later settings override earlier ones, callback errors are
joined, and failed options prevent dispatch. Cancellation is checked before
callbacks and again before allocation.

Parameter conversion accepts only Wire's portable subset: null, booleans,
signed integers, unsigned integers fitting `int64`, finite floats, strings,
bytes, `[]any`, and `map[string]any`. Integer and floating-point values remain
distinct. Empty parameter names, excessive nesting, non-finite numbers,
duration, datetime, regexp, and unsupported custom values are rejected locally.
The Universal API does not declare a portable parameter subset; this remains
a transport constraint, documented without introducing a parallel public type.

## Example

Transport construction is separate from runtime use. This function accepts
any local or remote Universal API runtime, borrowing it while owning the plan
and session it creates:

```go
func runQuery(ctx context.Context, runtime api.Runtime) (out *api.Output, err error) {
    plan, err := runtime.Compile(ctx, api.NewSource("query.fql", "RETURN @input"))
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, plan.Close()) }()

    session, err := plan.NewSession(ctx, api.WithParam("input", "hello"))
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, session.Close()) }()

    return session.Run(ctx)
}
```

Create a remote runtime with `client.New(ctx, target, options...)` or
`client.From(ctx, conn)` and close it after its resources. Only `From` leaves
`conn` caller-owned on every path, including startup failure, closure, and recovery.

## Debugger

`CompileDebug` followed by `Plan.NewDebugSession` returns
`api/debugger.Session`. `Start`, `Continue`, `StepIn`, `StepOver`, and `StepOut`
return canonical debugger events at the next stop or completion. The private
adapter serializes these commands and uses `RunCommand` streams. Start retains
its stream after the first stop so its context owns the hosted execution
lifetime. Each resume has its own caller context. There is no fallback to
legacy asynchronous command RPCs.

`Pause`, breakpoint operations, enumeration, inspection, and evaluation all
require a non-nil context. Cancellation is checked before and after admission
and reaches the corresponding hosted operation. Request cancellation does not
release the debugger. Explicit Close cancels and settles commands, closes the
hosted debugger, captures final breakpoint data or its enumeration error, and
releases the transport handle. Later `Breakpoints(ctx)` reads detached retained
data or returns that error while still checking the context.

`ReplaceBreakpoints` makes one atomic hosted call, including while running.
It preserves request order, duplicates, stable IDs, unresolved entries, and
default-source selection. Limits count the resulting set across all sources.
An ambiguous lost mutation reply never triggers a rollback. `SetBreakpoint`
and `SetBreakpointAt` remain distinct, as do `Locals`/`FrameLocals` and
`Evaluate`/`EvaluateFrame`.

Frame slice order defines the zero-based index for frame-local and evaluation
operations. Breakpoint IDs and value references remain public because they are
canonical debugger concepts; they do not identify Wire ownership scopes.
Positive references are usable only at the current stopped state; zero and
stale references are rejected. Source names are semantic identifiers, never
interpreted as local paths.

Breakpoints preserve requested/resolved locations, spans, binding mode,
point/function IDs, and bound state. Events preserve stop reason, depth, hit
breakpoint IDs, output, and failure. Runtime-error stops carry the failure in
`debugger.Event.Error`; command errors are separate and may accompany an event
and completion output. Cancellation and deadline errors retain `errors.Is`
identity. Function IDs include `debugger.NoFunction` (-1). Completion
and termination map to their canonical reasons, without a second event API.

## Allocation and cancellation

The Universal API adapter checks cancellation before sending an allocation
request, including after option callbacks. Only the acquisition RPC is detached
from caller cancellation, with an internal 30-second deadline. If its resource
handle arrives after cancellation, the adapter releases that handle before
returning the caller's cancellation. Execution waiting uses the original caller
context; releasing the temporary Execution also cancels unfinished work.

A lost reply or a reply without a usable resource ID cannot be reclaimed by ID.
The adapter explicitly invokes cascading transport release on the narrowest known owner: a Plan for an unknown
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

Operation and cleanup errors remain joined. Narrow recovery preserves the channel.
Recovery that invalidates the whole logical connection also closes a `New`-owned
channel; a `From` transport and other logical clients sharing it remain open. These bounds limit client waiting;
the hosted implementation must still honor its cancellation and Close contracts.

## Closing resources

The constructor context bounds the handshake, not the lifetime of the returned
runtime. Cancelling it after construction does not close the runtime. Startup cancellation
covers stream creation and initial receive, then detaches before successful
publication. Context values and outgoing metadata survive that separation.
Rollback uses fresh bounded detached cleanup and can add time after the startup
deadline; there is no constructor timeout option. Caller deadlines control startup.

Public resources implement `Close() error`. Closing uses a detached context
with a 30-second bound. The first close commits teardown exactly once.
Concurrent and repeated callers observe the retained release result; a caller
whose wait expires does not abandon committed cleanup. Failed releases remain
observable rather than being hidden or automatically retried.

`Plan.Close` gates new constructors, settles admitted constructors without
canceling them, and closes the hosted plan once through `ClosePlan`. Published
sessions and debuggers remain usable and closeable. The transport plan remains
retained until the last child finishes, then `ReleasePlan` removes it.

`Runtime.Close` immediately rejects new root calls while preserving admitted
calls and descendants. Its logical connection is released after the last
retained operation or plan finishes. If teardown was deferred, its later error
belongs to the operation or child close that performs the final release; it
never changes an earlier runtime-close result. Final release attempts bounded
logical cleanup and cancels the Connect stream before closing a `New`-owned channel.
Channel closure still occurs when cleanup fails or times out; independent errors
are joined. Definitive Connect termination follows the same exactly-once teardown.
Borrowed physical transport and the hosted runtime stay open.

`ReleasePlan`, `CloseConnection`, disconnect, and lost-allocation recovery
retain cascading semantics. Child admission ignores ordinary ancestor API
closure and observes transport release instead. Callers must close all children;
closing a parent is no longer a substitute for their cleanup.

Private watches are tied to operation and logical connection contexts. An
existing watch may receive the terminal event during resource closure; new
operations are rejected after closure begins. Private handles and cleanup
helpers remain with their existing lifecycle owners inside `client`.

## Errors

Immediate failures remain `*client.Error`, preserving `failure.Category`,
sanitized message, canonical `diagnostics.Diagnostics`, and the transport cause
through `Unwrap`. Categories are set only when the server supplies an
`ErrorDetail`; transport-native cancellation, deadlines, unavailable,
invalid-request, and resource-exhaustion errors keep category zero.
`status.Code(err)` remains available for transport-specific handling.
Wire never parses arbitrary error strings to reconstruct diagnostics.

Terminal execution and debugger failures remain `*failure.Failure`.
`client.ErrClosed` identifies closed logical resources.
`client.ErrExecutionCancelled` identifies remote execution cancellation and
remains distinct from cancellation of the caller's context.
Operation and cleanup errors are joined so `errors.Is` and `errors.As` can find
each component.

These Wire errors remain public because the Universal API has no equivalent
general remote-error taxonomy. Connection and allocation IDs remain private,
and contained implementation panic details remain sanitized.

## Migration

The Go constructor change is intentional: replace `client.New(ctx, conn)` with
`client.From(ctx, conn)` to preserve borrowed ownership. `NewRuntime` callers also
use `From`. Use `New(ctx, target, options...)` for an owned channel. Use `api.Runtime`, `api.Session`,
and `api.Output` instead of client aliases. The old `Client`, `Plan`,
`Execution`, `DebugSession`, and event receiver types, semantic option structs,
`Parameters`, `RuntimeInfo`, and `Capabilities` have been removed without
compatibility shims.

Use canonical runtime/plan/session operations, cancellation contexts, and
debugger events. The versioned protobuf services and shared domain packages
are extended additively for alpha.19 and alpha.20; callers implementing protocol
tooling may still use the generated bindings directly. See the
[alpha.20 protocol contract](protocol.md#alpha20-metadata-adoption).
