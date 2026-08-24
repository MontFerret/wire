# Client Architecture

The handwritten Go client is a domain facade over the Wire protocol. Its
implementation has three layers with one dependency direction:

```text
Public client domain API
          ↓
Private Wire protocol adapter
          ↓
Generated protobuf and gRPC client
```

The adapter is a concrete, unexported type in the `client` package. Keeping the
boundary in the same Go package lets it produce the existing client-domain
types without exporting an implementation interface, creating a package
cycle, or maintaining a second internal model. Source files and ownership make
the boundary explicit: domain files do not construct or inspect generated
messages, while `protocol_*.go` files contain protocol mechanics.

## Public domain layer

The public domain layer owns `Client`, `Plan`, `Execution`, `DebugSession`, and
their options, states, events, snapshots, results, and errors. It owns typed
resource relationships, user-facing lifecycle, immutable handle metadata, and
convenience composition such as `Client.Run`, `Plan.Run`, and
`Execution.Wait`.

This layer speaks in Wire and Ferret concepts. It decides whether a handle is
open, which ancestor owns cleanup, how terminal execution states map to
`Output` or `Failure`, and how operation and cleanup errors are joined. It does
not construct protobuf requests, propagate protocol IDs into messages, invoke
generated RPC clients, or interpret generated responses.

`Client` owns one logical Wire connection but borrows the caller's physical
gRPC transport. `Plan` owns its executions and debug sessions. Closing an
ancestor makes descendants unusable; a descendant closed after ancestor
cleanup starts observes the ancestor's retained release result instead of
issuing a redundant release RPC.

## Protocol adapter layer

The private protocol adapter owns the generated service clients, opaque
connection ID, Connect stream mechanics, and all request and response
adaptation. Its responsibilities are:

- constructing protobuf requests and propagating connection and resource IDs;
- invoking the generated Runtime, Plan, Execution, and Debug service clients;
- encoding public parameters into protocol values;
- converting protobuf metadata, diagnostics, failures, output, and events into
  client-domain values;
- validating required response shapes; and
- normalizing transport and structured Wire errors at the protocol boundary.

The adapter returns domain values or small private resource descriptions. It
does not decide public handle ownership, close ordering, convenience-method
composition, or terminal `Execution.Wait` policy. Generated service-client
interfaces provide the existing test seam; Wire does not add a parallel
adapter interface.

Execution watches use a dedicated private stream wrapper that receives one
generated event at a time and converts it to `ExecutionEvent`. There is no
generic streaming framework. The public receiver owns only domain concerns,
including ending its local watch after a terminal event.

## Generated transport layer

Generated files under `gen/ferret/wire/v1` own protobuf message shapes and gRPC
client mechanics. They are derived from `proto/ferret/wire/v1` and
`buf.gen.yaml`; they are never edited by hand. The adapter is the only
non-debug client execution layer that depends on these generated types.

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
release operation and checks its owning ancestor before invoking the protocol
adapter.

## Migration boundary

The non-debug path follows this architecture in Task 1. DebugSession keeps its
existing RPC and conversion implementation until Task 2, while sharing the new
Client storage and lifecycle primitive. This staged exception is intentional:
Task 1 must not partially redesign debugger commands, inspection, events, or
capabilities merely to make the source tree look uniform.
