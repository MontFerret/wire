# Wire Architecture

Wire is a security-sensitive RPC boundary that exposes a host application's
configured Unified API runtime to external tooling. It adapts the API's
execution and debugger contracts across a process boundary without assuming a
particular runtime implementation.

## Boundaries and ownership

The package dependency direction is:

```text
consumer → client ────────────────┐
             ↕ protobuf/gRPC      ├→ pkg/execution, pkg/debugger, pkg/failure
host → server → server/internal ──┘→ Unified API → runtime implementation
```

| Concern | Owner |
| --- | --- |
| FQL, runtime, output encoding, and debugger semantics | Unified API and runtime implementation |
| Runtime construction, configuration, policies, and application state | Host application |
| Versioned RPC contract | Protobuf definitions |
| Shared execution, debugger, and failure semantics | `pkg/execution`, `pkg/debugger`, and `pkg/failure` |
| RPC adaptation | `server/internal/grpcserver` |
| Logical connections and resources | `server/internal/core` |
| Public server lifecycle | `server` package |
| Go client facade | `client` |
| Physical transport, listener, authentication, and TLS | Host and Wire server layer |
| DAP translation, LSP, and language intelligence | ferretd and compiler tooling |

Runtime implementations must never depend on Wire. Wire must not absorb DAP, LSP,
transport-security, or host-configuration semantics for downstream
convenience.

`client.New(ctx, conn)` returns the canonical `api.Runtime` interface.
Private adapters implement `api.Plan`, `api.Session`, and `api/debugger.Session`;
output is `api.Output`, whose definition belongs to `api/result`. The client
does not re-export aliases or expose a second resource or event model.
Its logical connection, allocation handles, RPC clients, and watches remain
private within the owning client package.

The caller supplies and owns the physical transport. Runtime and resource
`Close` methods release logical resources with bounded detached cleanup.
`server.NewServer` accepts `api.Runtime` directly. Optional host identity is
`server.RuntimeIdentity`, supplied through `WithRuntimeIdentity`.

## gRPC service composition

`grpcserver.Server` constructs and registers five dedicated implementations.
It contains only those service instances and owns no RPC handlers. Each service
embeds its corresponding generated service base and adapts one protocol domain.

| Service | Invocation and ownership |
| --- | --- |
| RuntimeService | Opens/closes logical connections; calls `core.Run` with the borrowed runtime and connection store |
| PlanService | Calls `core.CompilePlan` with the borrowed runtime and connection store; releases plans through that store |
| SessionService | Resolves a Plan in the connection store and calls its `NewSession` |
| ExecutionService | Resolves a Plan or Session for execution creation; resolves Execution for watches, cancellation, and release |
| DebugService | Resolves a Plan for debugger creation; resolves DebugSession for commands, inspection, watches, and release |

`prepareOperation` resolves the connection and returns its resource store plus
an ordinary `context.Context`. The context preserves request values and deadlines
and joins connection cancellation; each handler cancels it to detach the lifetime
callback. There is no dependency-carrying operation context.

Services convert protobuf sources and options to canonical API types before
calling core. Source/diagnostic, option/value, output, execution, debugger, and
failure conversions are grouped at the transport boundary. Handshake metadata
belongs to transport configuration, not resource management. Domain errors
supply shared Wire categories; gRPC owns status mapping and uses the same
category serialization as terminal failures. Canonical diagnostic extraction
is shared within server error handling.

## Execution and host boundaries

Normal results cross Wire through the Unified API encoded output abstraction:

```go
type Output struct {
	ContentType string
	Content     []byte
}
```

The contract is exactly content type plus encoded bytes. Wire does not expose,
reconstruct, or maintain a parallel representation of implementation-specific
runtime values. It does not add Wire-specific runtime options, private raw-value
codecs, runtime-value type switches, or locally reconstructed runtime behavior.
Plan executions use `api.Plan` and `api.Session`; the direct-runtime operation
calls the borrowed `api.Runtime.Run` exactly once. Debugger values are the
separate structured boundary defined by `api/debugger`.

The host supplies both the configured runtime and listener:

```go
hostRuntime := createApplicationRuntime()
wireServer, err := server.NewServer(hostRuntime)
err = wireServer.Serve(ctx, listener)
```

Wire borrows both. It does not close the runtime, construct or secure a
listener, or reconstruct the application's modules, functions, policies,
resources, or configuration. Importing Wire has no side effects, and
`NewServer` does not listen, bind, dial, or inspect the environment.

The Connect handshake identifies the Wire protocol and may include a
host-supplied runtime identity. It does not claim a Ferret version, runtime
capability set, module inventory, or runtime implementation metadata that the
Unified API cannot provide portably. The API's portable
`diagnostics.Diagnostics` collection is preserved, but it has no severity or
general structured runtime-error taxonomy. Wire keeps only categories needed
to operate the remote lifecycle and sanitizes implementation failures.

### External implementation panic boundary

Calls into the host-supplied implementations of `api.Runtime`, `api.Plan`,
`api.Session`, and `debugger.Session` cross an explicit panic boundary. The
boundary converts an implementation panic into a typed internal error that
retains the panic value and stack for diagnostics. Ordinary returned errors pass
through unchanged, and neither panic values nor stacks cross the sanitized
protocol boundary.

Session-option preparation occurs before entering the panic boundary, which
covers only the external method invocation. Wire validation,
registries, state transitions, snapshot construction, event publication,
bookkeeping, and lifecycle orchestration remain outside it. A panic in
Wire-owned code is a programming defect and follows the existing server or
detached-cleanup panic policy rather than being mislabeled as an implementation
failure.

Containment does not make a stateful implementation safe to reuse. A panic from
a temporary execution session fails that execution and the session is closed
exactly once. A panic from a durable normal Session fails the active Execution
and poisons the Session against subsequent runs until release closes it. Any
debugger operation panic before teardown fails the logical debug
session, publishes its terminal failure, rejects subsequent runtime commands,
and starts idempotent debugger cleanup. A constructor panic fails only the
attempted resource; it does not disable the borrowed runtime or invalidate its
parent plan. Cleanup panics are retained as cleanup errors, while teardown
continues across unrelated resources.

## Protocol contract

The versioned protobuf API, currently `ferret.wire.v1`, is canonical. Generated
Go bindings are derived artifacts. The current schema is an intentional
pre-stable v1 contract reset around the Unified API, not a v2 fork. Removed
field numbers, names, and enum values remain reserved. After this reset is
released, incompatible changes require deliberate versioning and Buf review.

Protocol messages remain independent of private Go implementation structures
and do not mirror DAP or LSP for downstream convenience. An incompatible
redesign requires a new protocol version and a Buf breaking check against the
intended base branch.

Connection, plan, normal-session, execution, and debug-session IDs are opaque and
server-issued. They are scoped to one logical connection and cannot be inferred
or transferred to another connection. The handwritten Go client keeps these
IDs private; see [Client Handles](client.md).

Execution and debugger snapshots use the canonical types in `pkg/execution` and
`pkg/debugger`; terminal failures use `pkg/failure`. Those shared packages
contain no connection, plan, normal-session, execution, or debug-session identity and do not
depend on client or server implementation packages. Debugger values use the
canonical `api/debugger` and `api/source` types. Protobuf messages remain
transport representations and are converted explicitly at the gRPC server and
client boundaries. Both directions validate coordinates, IDs, references, and
enum values before conversion; invalid runtime values become sanitized
internal failures, while malformed server responses become local client
errors.

Only errors typed as `diagnostics.Diagnostics` are converted. Wire does not
parse error strings or expose arbitrary causes. Immediate diagnostics are a
separate gRPC status detail; asynchronous diagnostics are stored on the
failure snapshot. Source content, semantic source names, ordered annotations,
ranges, kinds, hints, and notes remain intact.

Compilation uses `api.Source` throughout the client and core. The protocol has
cohesive `Source`, `Position`, `Span`, `Location`, and `Range` messages. Wire
validates coordinates and preserves span values without assigning units to
them. `Location.SourceName` and protocol `source_name` do not imply a local
filesystem path. Debugger transport preserves event depth, requested and resolved
breakpoint locations, binding and bound state, point and function IDs, frame
function IDs, variable flags, value references, stop reasons, and hit
breakpoint IDs. Frame order is the zero-based index accepted by frame-local and
evaluation calls; no redundant frame index is transmitted.

Parameters preserve exact signed `int64` separately from finite protobuf
doubles. Both adapters reject NaN and infinities, including when nested.
Positive debugger value references are valid only in the current stopped state;
zero is never a request reference, and resume makes prior references stale.

The complete RPC and message contract, classification audit, and known Unified
API gaps are documented in [Wire Protocol](protocol.md).

## Logical lifecycle and concurrency

A Wire connection is the logical ownership scope established by the long-lived
`RuntimeService.Connect` stream, not a physical HTTP/2 or socket connection:

```text
Wire connection
├── direct runtime executions
└── plans
    ├── direct executions
    ├── normal sessions
    │   └── one active execution
    └── debug sessions
```

`ConnectionRegistry` is the only server-wide resource index. It owns connection
capacity, active/closing membership, and shutdown admission. `Connection` owns
its ID, cancellation context, retained close result, and one `ResourceStore`.

The store contains typed maps for plans, normal sessions, executions, and debug
sessions. IDs resolve only in the requesting connection's store; there are no
global resource maps or owner-ID indexes. Plans hold their child collections,
normal sessions hold their active execution, and children retain direct parent
references. Resources remain in the store while closing and are removed only
when cleanup settles.

`CompilePlan` and `Run` take the borrowed `api.Runtime` and store explicitly.
They own root allocation; neither introduces another runtime wrapper. A Plan
owns its hosted `api.Plan`, parameter metadata, child creation, and descendant
cleanup. A normal Session owns its hosted `api.Session`, poisoning state, and
execution admission. Execution owns asynchronous work, snapshots, cancellation,
and watches. DebugSession directly owns its hosted `debugger.Session`, command
state, breakpoint bookkeeping, watches, and close. No operation managers,
debugger controller, or cross-resource lifecycle manager intervene.

The client adapter uses the same ownership tree to reclaim allocations whose
responses are lost. Unknown Session IDs invalidate their Plan; unknown
Session-owned Execution IDs invalidate their Session; unknown root allocations
invalidate the logical Runtime. For an unknown child, a failed narrow release
advances to the next known ancestor before logical connection cleanup:
Session → Plan → Runtime. Connection cleanup includes Connect-stream
cancellation when release cannot be acknowledged. Known-ID release failures
retain and return the cleanup error without automatic ancestor invalidation.
An undelivered release can leave the hosted child until explicit ancestor
cleanup; a lost acknowledgement after committed cleanup permits Session reuse.
Acquisition and automatic release waits each have a 30-second bound. Successful
narrow cleanup preserves siblings outside its subtree and never closes the
borrowed physical transport. See [Client Handles](client.md)
for the cancellation contract.

`Execution` and `DebugSession` share a private event stream that owns sequence
allocation, latest-event replay, bounded buffers, subscription accounting, lag
eviction, and channel shutdown. It has no execution/debugger semantics.
DebugSession also retains a cohesive state value and breakpoint set; the set
owns the Wire limit and successful breakpoint records.

Creation reserves capacity before invoking the hosted API. Pending, published,
and closing resources all count toward the connection's limit. Publication
checks request cancellation, connection admission, and live ancestors under
the store mutex. Failure or abandonment closes returned hosted resources before
releasing the pending reservation. Constructors return real resource handles;
shared snapshots carry no ownership identity.

The store mutex protects maps, reservations, parent links, and allocation/release
admission. Creation gates are incremented under that mutex before release can
start waiting. Connection cancellation shares this admission lock. Resource
state locks must never be held while acquiring the store mutex. Hosted calls,
recursive release, and cleanup waits run without it.

Release belongs to each resource. Plan release gates new descendants and waits
for admitted constructors, releases executions, normal sessions, and debug
sessions, then closes its hosted plan. Session release cancels its lifetime,
waits for execution publication, releases its execution, then closes its hosted
session. The execution slot remains occupied until release finishes, even after
a terminal result. Execution/debugger release detaches storage only after local
cleanup settles. All removals also update direct parent links.

Connection teardown cancels in-flight work, closes store admission, waits for
pending creation, and settles executions, sessions, debuggers, and plans. Server
shutdown rejects new connections and closes the existing connections. Neither
path closes the borrowed runtime.

DebugSession has separate operation and state mutexes. Its operation mutex
serializes stopped-state commands, inspection, breakpoint bookkeeping, pause,
and command completion. The breakpoint set uses that mutex without adding a
redundant lock. The state mutex protects snapshots and transitions and never
spans a hosted API call.

Release is committed teardown. Concurrent callers observing the same in-flight
release wait for its retained result. After teardown finishes, the resource ID
is stale and returns the relevant structured not-found error; permanent
tombstones are not retained. Logical ownership is not coupled to grpc-go
transport internals, peer addresses, or socket identity. Leases, TTLs,
heartbeats, and reconnect tokens require a separate explicit contract.

Every stateful resource has explicit synchronization, cancellation, ownership,
and termination. Context cancellation propagates into Unified API operations.
Debug inspection cannot wait through a resume and then inspect a later stop.
An asynchronous resume releases the operation mutex while the runtime command
is active so `Pause` and close can reach the hosted debugger. Command completion
reacquires the operation mutex before committing state, which keeps pause
responses and event ordering deterministic. Close cancels the session and calls
the hosted debugger without waiting behind a potentially blocking stopped-state
operation, then serializes the final state and event commit.

Event buffers are bounded and producers are non-blocking. Each watch first
replays the latest published snapshot, then receives ordered
changes through one terminal snapshot. Cancelling or disconnecting a watch
detaches only that watcher; execution and debugging continue under their
resource lifecycle. Slow clients cannot block runtime work or create unbounded
queues and are detached with resource exhaustion. Watcher slots remain owned
until the stream handler exits, including after lag or a terminal snapshot.
Detached cleanup has a named owner, is panic-safe, and terminates
deterministically.

Each resource release follows the existing detached-release terminal policy:
its recovery settles release waiters and store bookkeeping if Wire
orchestration panics. This is distinct from `panicboundary`, which guards only
external implementation calls. Normal-session release invokes the hosted
`api.Session.Close` through that boundary and retains the cleanup result.

Direct Plan execution, normal Session run, and direct Runtime run construction
publish running state. Debug-session construction
publishes created state before the resource is returned, so every fresh debug
watch has a snapshot without adding a Get RPC. Start, continue, and the three
canonical step operations publish running before invoking the debugger; their
completion then publishes stopped or terminal state with a monotonic sequence.

## Limits and security

Every Wire server is a potential remote-code-execution boundary, including over
local IPC. Requests and lifecycle identifiers are untrusted.

`DefaultLimits` supplies the secure baseline:

| Resource | Default limit |
| --- | ---: |
| Logical connections | 64 |
| Plans per connection | 128 |
| Normal sessions per connection | 128 |
| Executions per connection | 128 |
| Debug sessions per connection | 32 |
| Watch streams per execution or debug session | 8 |
| Breakpoints per debug session | 256 |
| Inbound gRPC message | 4 MiB |
| Outbound gRPC message | 4 MiB |

Hosts may replace the complete positive set with `WithServerLimits`. Pending,
active, and closing resources all count. Implementations validate identifiers,
required fields, ranges, and state; bound client-controlled allocations; and
sanitize internal failures and panic values. They do not leak unnecessary host
details, trust client-provided ownership, bypass host runtime policies, or
introduce limit bypasses.

Portable diagnostics may contain the source text and semantic name supplied to
the runtime. Hosts should treat those fields like the original request. Runtime
error strings, panic values, stacks, and implementation-specific diagnostic
objects remain behind the sanitization boundary.

Listener exposure, peer authentication, authorization, and TLS remain host
transport concerns until separately specified. Wire never creates an unsafe
default endpoint.

## Scope

Wire remains a narrow bridge. It is not another Ferret runtime, a DAP or LSP
implementation, a module registry, a plugin manager, an application framework,
or a distributed execution system. New responsibilities require a concrete
architectural reason and an explicit contract.
