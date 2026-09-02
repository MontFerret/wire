# Wire Architecture

Wire is a security-sensitive RPC boundary that exposes a host application's
configured Unified API runtime to external tooling. It adapts the API's
execution and debugger contracts across a process boundary without assuming a
particular runtime implementation.

## Boundaries and ownership

The dependency direction is:

```text
consumers → protobuf/gRPC → Wire components and lifecycle → Unified API → runtime implementation
```

| Concern | Owner |
| --- | --- |
| FQL, runtime, output encoding, and debugger semantics | Unified API and runtime implementation |
| Runtime construction, configuration, policies, and application state | Host application |
| Versioned RPC contract | Protobuf definitions |
| RPC adaptation | `internal/grpcserver` |
| Logical connections and resources | `internal/core` |
| Public server lifecycle | Top-level `wire` package |
| Go client facade | `client` |
| Physical transport, listener, authentication, and TLS | Host and Wire server layer |
| DAP translation, LSP, and language intelligence | ferretd and compiler tooling |

Runtime implementations must never depend on Wire. Wire must not absorb DAP, LSP,
transport-security, or host-configuration semantics for downstream
convenience.

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
codecs, runtime-value type switches, or execution paths that bypass `api.Plan`
and `api.Session`. Debugger values are the separate structured boundary defined
by `api/debugger`.

The host supplies both the configured runtime and listener:

```go
runtime := createApplicationRuntime()
server, err := wire.NewServer(runtime)
err = server.Serve(ctx, listener)
```

Wire borrows both. It does not close the runtime, construct or secure a
listener, or reconstruct the application's modules, functions, policies,
resources, or configuration. Importing Wire has no side effects, and
`NewServer` does not listen, bind, dial, or inspect the environment.

The Connect handshake identifies the Wire protocol and may include a
host-supplied runtime identity. It does not claim a Ferret version, runtime
capability set, module inventory, or runtime implementation metadata that the
Unified API cannot provide portably. The current API also has no neutral
diagnostic or error taxonomy; Wire keeps only categories needed to operate the
remote lifecycle and sanitizes implementation failures.

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

Connection, plan, execution, and debug-session IDs are opaque and
server-issued. They are scoped to one logical connection and cannot be inferred
or transferred to another connection. The handwritten Go client keeps these
IDs private; see [Client Handles](client.md).

Core debugger state and client inspection values use the canonical
`api/debugger` and `api/source` types. Protobuf messages remain transport
representations and are converted explicitly at the gRPC server and client
boundaries. Both directions validate coordinates, IDs, references, and enum
values before conversion; invalid runtime values become sanitized internal
failures, while malformed server responses become local client errors.

Compilation uses `api.Source` throughout the client and core. The protocol has
cohesive `Source`, `Position`, `Span`, `Location`, and `Range` messages. Wire
validates coordinates and preserves span values without assigning units to
them. Debugger transport preserves event depth, requested and resolved
breakpoint locations, binding and bound state, point and function IDs, frame
function IDs, variable flags, value references, stop reasons, and hit
breakpoint IDs. Frame order is the zero-based index accepted by frame-local and
evaluation calls; no redundant frame index is transmitted.

The complete RPC and message contract, classification audit, and known Unified
API gaps are documented in [Wire Protocol](protocol.md).

## Logical lifecycle and concurrency

A Wire connection is the logical ownership scope established by the long-lived
`RuntimeService.Connect` stream, not a physical HTTP/2 or socket connection:

```text
Wire connection
└── plans
    ├── executions
    └── debug sessions
```

Internally, `Connection` owns only its opaque ID, cancellation context, open or
closing state, and admission of in-flight operations. Server-scoped
`ConnectionRegistry`, `PlanRegistry`, `ExecutionRegistry`, and
`DebugSessionRegistry` instances own storage, indexes, and capacity accounting.
Every plan, execution, and debug session records its owning connection ID;
children also record their parent plan ID. An ID lookup always includes the
requesting connection, so knowledge of another connection's ID never grants
access.

The server-scoped `Compiler`, `Executor`, and `Debugger` components own resource
creation. Only `Compiler` depends on `api.Runtime`; execution and debugging use
the `api.Plan` obtained from `PlanRegistry`. `Lifecycle` owns cleanup spanning
resource types. Individual resources retain their own state machines, runtime
handles, watches, and local close invariants. A per-operation Wire `Context`
combines the unary or stream context with the resolved logical connection.

```text
Compiler ──► api.Runtime
Compiler ──► PlanRegistry ◄── Executor
                            ◄── Debugger
Executor ──► ExecutionRegistry
Debugger ──► DebugSessionRegistry
Lifecycle ──► all four registries

ConnectionRegistry ──► Connection ◄── operation Context
```

The arrows show dependencies: components depend on registries, registries do
not depend on components, and `Connection` has no dependency on either.

Creation uses reserve, create, and commit phases. Pending capacity is reserved
before calling the Unified API, registry locks are released for runtime calls,
and publication is committed only while the connection and parent plan still
accept children. Plan release gates new children, waits for in-flight child
constructors, releases executions and debug sessions, and only then closes the
Unified API plan.

Each registry owns its collection lock and each resource owns its state lock.
The only nested publication order is plan registry, plan, then the child
registry. Connection shutdown first closes operation admission and waits for
admitted creation to settle. Release paths never hold registry locks while
waiting for constructors, children, or Unified API cleanup. Debug-session state
locking remains resource-local so commands and inspection stay serialized
without coupling unrelated registries.

When the Connect stream terminates, cleanup rejects new operations and cancels
in-flight creation, waits for creation to settle, cancels and releases
executions, closes debug sessions, releases plans, and terminates owned state
and goroutines. Parent and connection traversal uses registry owner and plan
indexes rather than nested resource collections.

Release is committed teardown. Concurrent callers observing the same in-flight
release wait for its retained result. After teardown finishes, the resource ID
is stale and returns the relevant structured not-found error; permanent
tombstones are not retained. Logical ownership is not coupled to grpc-go
transport internals, peer addresses, or socket identity. Leases, TTLs,
heartbeats, and reconnect tokens require a separate explicit contract.

Every stateful resource has explicit synchronization, cancellation, ownership,
and termination. Context cancellation propagates into Unified API operations.
Debug inspection cannot wait through a resume and then inspect a later stop.
Wire uses its explicit state lock and the Unified API debugger session rather
than introducing a second command scheduler.

Event buffers are bounded and producers are non-blocking. Each watch first
replays the latest published snapshot when one exists, then receives ordered
changes through one terminal snapshot. Cancelling or disconnecting a watch
detaches only that watcher; execution and debugging continue under their
resource lifecycle. Slow clients cannot block runtime work or create unbounded
queues and are detached with resource exhaustion. Watcher slots remain owned
until the stream handler exits, including after lag or a terminal snapshot.
Detached cleanup has a named owner, is panic-safe, and terminates
deterministically.

## Limits and security

Every Wire server is a potential remote-code-execution boundary, including over
local IPC. Requests and lifecycle identifiers are untrusted.

`DefaultServerLimits` supplies the secure baseline:

| Resource | Default limit |
| --- | ---: |
| Logical connections | 64 |
| Plans per connection | 128 |
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

Listener exposure, peer authentication, authorization, and TLS remain host
transport concerns until separately specified. Wire never creates an unsafe
default endpoint.

## Scope

Wire remains a narrow bridge. It is not another Ferret runtime, a DAP or LSP
implementation, a module registry, a plugin manager, an application framework,
or a distributed execution system. New responsibilities require a concrete
architectural reason and an explicit contract.
