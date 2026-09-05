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
hosted methods. `Plan.Params` returns a defensive copy. Each `NewSession`
creates one durable hosted session with the supplied semantic options;
sequential `Session.Run` calls reuse it. A concurrent run on that session is
rejected until the previous invocation's temporary execution has been released.
Distinct sessions and plans may execute concurrently.

Each runtime/session invocation privately acquires, watches, and releases an
execution. Output remains `api.Output`: its content type and encoded bytes are
copied without interpretation. No IDs, RPC handles, execution snapshots, or
connection metadata are exposed by the returned API interfaces.

## Options and parameters

Use `api.WithOptimizationLevel` for plan compilation and `api.WithParam`,
`api.WithParams`, and `api.WithOutputContentType` for direct runs and session
creation. There are no Wire-specific semantic option structs.

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
func runQuery(ctx context.Context, runtime api.Runtime) (out api.Output, err error) {
    plan, err := runtime.Compile(ctx, api.NewSource("query.fql", "RETURN @input"))
    if err != nil {
        return api.Output{}, err
    }
    defer func() { err = errors.Join(err, plan.Close()) }()

    session, err := plan.NewSession(ctx, api.WithParam("input", "hello"))
    if err != nil {
        return api.Output{}, err
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
adapter serializes these commands and consumes Wire watches internally.

`Pause`, breakpoint operations, `Frames`, `Locals`, `FrameLocals`, `Variables`,
`Evaluate`, and `EvaluateFrame` retain their canonical signatures. Operations
without a caller context use the debugger's lifetime context; closing the
debugger cancels its pending work. Cancelling a resume command closes the
debugger and joins any cleanup error with caller cancellation.

Frame slice order defines the zero-based index for frame-local and evaluation
operations. Breakpoint IDs and value references remain public because they are
canonical debugger concepts; they do not identify Wire ownership scopes.
Positive references are usable only at the current stopped state; zero and
stale references are rejected. Source names are semantic identifiers, never
interpreted as local paths.

Breakpoints preserve requested/resolved locations, spans, binding mode,
point/function IDs, and bound state. Events preserve stop reason, depth, hit
breakpoint IDs, output, and failure. Runtime-error stops carry the failure in
`debugger.Event.Error`; failed debugger commands return an error. Completion
and termination map to their canonical reasons, without a second event API.

## Allocation and cancellation

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

## Closing resources

The constructor context bounds the handshake, not the lifetime of the returned
runtime. Cancelling it after construction does not close the runtime.

Public resources implement `Close() error`. Closing uses a detached context
with a 30-second bound. The first close commits teardown exactly once.
Concurrent and repeated callers observe the retained release result; a caller
whose wait expires does not abandon committed cleanup. Failed releases remain
observable rather than being hidden or automatically retried.

Closing a runtime or plan owns descendant cleanup. Descendant operations are
rejected as soon as ancestor closure begins. A descendant closed after ancestor
cleanup begins observes the ancestor's retained result instead of issuing a
duplicate release. Close children before parents when each direct cleanup
result matters; normal defer ordering provides this.

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
remain unchanged; callers implementing protocol tooling may still use the
generated bindings directly.
