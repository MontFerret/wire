# Wire Architecture

Wire is a security-sensitive RPC boundary that exposes a host application's
configured Ferret engine to external tooling. It adapts Ferret's public
execution and debugger APIs across a process boundary without redefining their
semantics.

## Boundaries and ownership

The dependency direction is:

```text
ferret
   ↑
wire
   ↑
consumers
```

| Concern | Owner |
| --- | --- |
| FQL, runtime, output encoding, and debugger semantics | Ferret |
| Engine construction, configuration, policies, and application state | Host application |
| Versioned RPC contract | Protobuf definitions |
| RPC adaptation | `internal/grpcserver` |
| Logical connections and resources | `internal/core` |
| Public server lifecycle | Top-level `wire` package |
| Go client facade | `client` |
| Physical transport, listener, authentication, and TLS | Host and Wire server layer |
| DAP translation, LSP, and language intelligence | ferretd and compiler tooling |

Ferret core must never depend on Wire. Wire must not absorb DAP, LSP,
transport-security, or host-configuration semantics for downstream
convenience.

## Execution and host boundaries

Normal results cross Wire through Ferret's encoded output abstraction:

```go
type Output struct {
	ContentType string
	Content     []byte
}
```

The contract is exactly content type plus encoded bytes. Wire does not expose,
reconstruct, or maintain a parallel representation of internal Ferret runtime
values. It does not add Wire-specific engine options, private raw-value codecs,
runtime-value type switches, or execution paths that bypass Ferret's public
`Plan` and `Session` APIs. Debugger values are a separate structured boundary
defined by Ferret's public debugger API.

The host supplies both the configured engine and listener:

```go
engine := createApplicationEngine()
server, err := wire.NewServer(engine)
err = server.Serve(ctx, listener)
```

Wire borrows both. It does not close the engine, construct or secure a
listener, or reconstruct the application's modules, functions, policies,
resources, or configuration. Importing Wire has no side effects, and
`NewServer` does not listen, bind, dial, or inspect the environment.

## Protocol contract

The versioned protobuf API, currently `ferret.wire.v1`, is canonical. Generated
Go bindings are derived artifacts. Within a released version, changes are
additive: field numbers and reserved names are not reused, meanings and types
are not changed incompatibly, and existing fields or RPCs are not removed
without deliberate versioning.

Protocol messages remain independent of private Go implementation structures
and do not mirror DAP or LSP for downstream convenience. An incompatible
redesign requires a new protocol version and a Buf breaking check against the
intended base branch.

Connection, plan, execution, and debug-session IDs are opaque and
server-issued. They are scoped to one logical connection and cannot be inferred
or transferred to another connection. The handwritten Go client keeps these
IDs private; see [Client Handles](client.md).

## Logical lifecycle and concurrency

A Wire connection is the logical ownership scope established by the long-lived
`RuntimeService.Connect` stream, not a physical HTTP/2 or socket connection:

```text
Wire connection
└── plans
    ├── executions
    └── debug sessions
```

When the Connect stream terminates, cleanup rejects new operations and cancels
in-flight creation, waits for creation to settle, closes debug sessions,
cancels and releases executions, releases plans, and terminates owned state and
goroutines.

Release is committed teardown. Concurrent callers observing the same in-flight
release wait for its retained result. After teardown finishes, the resource ID
is stale and returns the relevant structured not-found error; permanent
tombstones are not retained. Logical ownership is not coupled to grpc-go
transport internals, peer addresses, or socket identity. Leases, TTLs,
heartbeats, and reconnect tokens require a separate explicit contract.

Every stateful resource has explicit synchronization, cancellation, ownership,
and termination. Context cancellation propagates into Ferret operations where
its public API accepts a context. Debug inspection cannot wait through a resume
and then inspect a later stop. Wire uses its explicit state lock and Ferret's
serialized debugger API rather than introducing a second command scheduler.

Event buffers are bounded and producers are non-blocking. Slow clients cannot
block Ferret execution or create unbounded queues. Watcher slots remain owned
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
details, trust client-provided ownership, bypass Ferret filesystem or network
policies, or introduce limit bypasses.

Listener exposure, peer authentication, authorization, and TLS remain host
transport concerns until separately specified. Wire never creates an unsafe
default endpoint.

## Scope

Wire remains a narrow bridge. It is not another Ferret runtime, a DAP or LSP
implementation, a module registry, a plugin manager, an application framework,
or a distributed execution system. New responsibilities require a concrete
architectural reason and an explicit contract.
