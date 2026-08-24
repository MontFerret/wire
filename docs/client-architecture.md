# Client Architecture

The handwritten Go client is a domain facade over the Wire protocol. Its
implementation has three layers with one dependency direction:

```text
Public client domain API
          ↓
Private session and capability transports
          ↓
Generated protobuf and gRPC client
```

The private collaborators are concrete, unexported types in the `client`
package. Keeping the boundary in the same package lets them produce the
existing client-domain types without exporting implementation interfaces,
creating a package cycle, or maintaining a second internal model. Source files
and ownership make the boundary explicit: domain-oriented files do not
construct or inspect generated messages.

## Public domain layer

The public domain layer owns four cohesive resource objects:

- `Client` owns the logical Wire client lifecycle, exposes runtime metadata,
  creates plans, and composes one-shot runs.
- `Plan` represents one compiled program, exposes immutable metadata, creates
  executions and debug sessions, and composes plan-owned runs.
- `Execution` owns cancellation, watching, terminal-state waiting, and release
  policy for one execution.
- `DebugSession` owns debugger commands, local argument validation,
  observation, inspection, and release policy.

These objects own typed resource relationships, user-facing lifecycle,
immutable handle metadata, terminal-state policy, and cleanup error joining.
They speak in `Output`, `ExecutionEvent`, `DebugEvent`, `Frame`, `Variable`,
and other client-domain values. They do not construct protobuf requests,
propagate IDs into messages, invoke generated RPC clients, or inspect generated
responses.

`Client` owns one logical Wire connection but borrows the caller's physical
gRPC transport. `Plan` owns its executions and debug sessions. Closing an
ancestor makes descendants unusable; a descendant closed after ancestor
cleanup starts observes the ancestor's retained release result instead of
issuing a redundant release RPC.

## Private collaborator layer

`Client` composes one connection-scoped `session` with `planTransport`,
`executionTransport`, and `debugTransport` collaborators. Constructors assemble
complete domain resources with the owning parent, relevant transport, private
remote ID, immutable metadata, and lifecycle handle.

`session` performs the RuntimeService Connect and CloseConnection protocol. It
retains the opaque connection ID, owns the Connect stream and its cancellation,
and converts the handshake metadata. It does not own public handle state or
parent/child cleanup policy.

Each capability transport has one protocol responsibility:

- `planTransport` compiles and releases plans;
- `executionTransport` executes, cancels, watches, and releases executions; and
- `debugTransport` creates, commands, inspects, watches, and releases debug
  sessions.

These transports construct requests, propagate private IDs, invoke their
generated service client, validate required response shapes, convert responses
into domain values, and normalize protocol failures. Execution and debug
watches use resource-specific stream wrappers that receive and convert one
generated event at a time. There is no generic streaming framework.

Parameter encoding and structured error/diagnostic mapping remain shared
because both cross capability boundaries. Execution and debug conversions stay
with their owning transport; encoded output conversion is shared only for the
common `content type + bytes` contract.

There is deliberately no monolithic protocol facade and no broad handwritten
protocol interface. Generated capability-specific client interfaces are the
test seam. A change to execution adaptation does not require editing plan or
debug transport infrastructure.

## Generated transport layer

Generated files under `gen/ferret/wire/v1` own protobuf message shapes and gRPC
client mechanics. They are derived from `proto/ferret/wire/v1` and
`buf.gen.yaml`; they are never edited by hand. Only the session, capability
transports, protocol codecs, and protocol mappings depend on generated types.

The caller continues to own dialing, authentication, TLS, and closing the
physical `grpc.ClientConnInterface`. The client owns only the logical Connect
scope established over that transport.

## Lifecycle machinery

`internal/lifecycle` supplies protocol-agnostic synchronization for the common
open, closing, and closed handle states. The first Close commits one release;
concurrent and repeated callers wait for its retained result. A waiter's
context can stop that caller from waiting without cancelling the committed
cleanup. Cleanup detaches cancellation while preserving the initiating
deadline, and panic values are not exposed.

The lifecycle primitive knows nothing about resource IDs, protobuf, gRPC,
Ferret, or parent/child ownership. Each domain resource supplies its own
release operation and checks its owning ancestor before invoking its capability
transport. Closing a Client or Plan therefore remains domain policy rather
than transport behavior.
