# Client API

`client.New(ctx, conn)` returns a remote implementation of `api.Runtime`.
The caller configures the endpoint, credentials, TLS, dialer, and transport
limits on the supplied `grpc.ClientConnInterface` and retains its ownership.
There is no second handwritten Wire resource API.

## Resource model

```text
api.Runtime
└── api.Plan
    ├── api.Session
    └── api/debugger.Session
```

All implementations remain private. Sources, options, output, diagnostics,
breakpoints, locations, frames, variables, reasons, and debugger events use
their canonical Universal API types directly. `client` exports only `New`,
`Error`, `ErrClosed`, and `ErrExecutionCancelled`.

`Runtime.Run` invokes the hosted `api.Runtime.Run` directly, once per call.
`Compile` and `CompileDebug` create reusable plans through the corresponding
hosted methods. `Plan.Params() ([]string, error)` returns a defensive copy of metadata captured
before the compiled plan was published. A hosted metadata error or panic closes
the unpublished plan once and preserves its cleanup error. Each `NewSession`
creates one durable hosted session with the supplied semantic options;
sequential `Session.Run` calls reuse it. A concurrent run on that session is
rejected until the previous invocation's temporary execution has been released.
Distinct sessions and plans may execute concurrently.

Each runtime/session invocation privately acquires, watches, and releases an
execution. Output is `*api.Output`: its content type and encoded bytes are
copied without interpretation. Nil output differs from present empty output,
and available output survives execution or cleanup errors. A hosted `(nil, nil)`
result is invalid. No IDs, RPC handles, execution snapshots, or
connection metadata are exposed by the returned API interfaces.

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

Create the remote runtime with `client.New(ctx, conn)` and close it after its
resources. Closing it never closes `conn`. Constructor failure returns a nil
`api.Runtime` interface and the decoded error.

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

Operation and cleanup errors remain joined. The caller-owned physical transport
and other logical clients on it remain open. These bounds limit client waiting;
the hosted implementation must still honor its cancellation and Close contracts.

## Closing resources

The constructor context bounds the handshake, not the lifetime of the returned
runtime. Cancelling it after construction does not close the runtime.

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
never changes an earlier runtime-close result. The borrowed physical transport
and hosted runtime stay open.

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

Call `client.New` instead of `NewRuntime`. Use `api.Runtime`, `api.Session`,
and `api.Output` instead of client aliases. The old `Client`, `Plan`,
`Execution`, `DebugSession`, and event receiver types, semantic option structs,
`Parameters`, `RuntimeInfo`, and `Capabilities` have been removed without
compatibility shims.

Use canonical runtime/plan/session operations, cancellation contexts, and
debugger events. The versioned protobuf services and shared domain packages
are extended additively for alpha.19; callers implementing protocol tooling may
still use the generated bindings directly. See the [contract audit](uapi-audit.md).
