# AGENTS.md

This file is the canonical operating guide for coding agents working in the
Wire repository. Wire is a security-sensitive RPC boundary that exposes a host
application's configured Ferret engine to external tooling. Preserve that
narrow responsibility.

## Sources of truth

Use the most direct repository authority for facts that can change:

* `go.mod` owns the module path, minimum Go version, and Ferret dependency.
* `Makefile` owns canonical development commands and pinned tool invocations.
* Protobuf sources under `proto/ferret/wire/v1` own Wire protocol semantics.
* `buf.yaml` owns protobuf lint and breaking-change policy.
* `buf.gen.yaml` owns protobuf inputs, generator versions, and output layout.
* Generated protobuf and gRPC bindings under `gen/ferret/wire/v1` are derived
  artifacts only.
* `.github/workflows/ci.yml` owns repository CI coverage and tested platforms.
* Current handwritten code and tests own implementation behavior.
* `README.md` owns repository-facing architecture, security, API examples, and
  development workflow documentation.

The current module is `github.com/MontFerret/wire`, uses Go 1.25, and adapts the
Ferret v2 dependency declared in `go.mod`. Verify these values rather than
copying them into implementation logic.

When generated bindings disagree with protobuf source, protobuf source is
authoritative. When descriptive documentation disagrees with current source or
tests, verify the intended contract and correct the stale documentation.

## Architecture Documentation

Keep architectural and design documentation in the repository's dedicated `docs/` directory.

`AGENTS.md` must remain concise and focused on contributor/agent instructions. Do not use it as the primary location for detailed architecture documentation, protocol design explanations, lifecycle descriptions, or design rationale.

When architectural knowledge needs to be documented:

1. Create or update an appropriate document under `docs/`.
2. Organize documents by architectural concern where useful, for example:

```text
docs/
  architecture.md
  protocol.md
  client.md
  lifecycle.md
```

Do not mechanically create all of these files; use only the structure justified by the current documentation.

3. Keep `AGENTS.md` limited to short architectural guidance and references to the authoritative documents, for example:

```markdown
## Architecture

Architecture and design documentation lives under [`docs/`](docs/).

Before making architectural changes, read the relevant documents there and update them when the change modifies documented behavior or design decisions.
```

4. Treat documentation under `docs/` as part of the implementation. When a change affects a documented architectural contract, update the relevant document in the same change.

5. Avoid duplicating detailed architectural information between `AGENTS.md` and `docs/`. `AGENTS.md` should point to the authoritative documentation rather than reproduce it.

As part of this task, inspect the current `AGENTS.md`. If detailed Wire architecture documentation has already accumulated there, move it into appropriately scoped documents under `docs/` and replace it with concise references.

Apply this convention to future architectural documentation as well.

## Architecture and ownership

The fundamental dependency direction is:

```text
ferret
   ↑
wire
   ↑
consumers
```

Ferret core must never depend on Wire. Wire adapts Ferret's public execution and
debugger APIs across a process boundary; it does not redefine them.

| Concern | Owner |
| --- | --- |
| FQL and runtime semantics | Ferret |
| Engine construction and configuration | Host application |
| Output encoding semantics | Ferret |
| Debugger semantics | Ferret |
| Versioned Wire contract | Protobuf definitions |
| RPC adaptation | `internal/grpcserver` |
| Logical connections and resources | `internal/core` |
| Public server lifecycle | Top-level `wire` package |
| Thin Go client facade | `client` |
| Physical gRPC transport and listener | Host and Wire server layer |
| DAP translation | ferretd, not Wire |
| LSP and language intelligence | ferretd/compiler tooling, not Wire |

Start in the layer that owns the requested behavior. Do not move Ferret, DAP,
LSP, transport, or host-configuration semantics into Wire for convenience.

## Execution boundary

Normal Ferret results cross Wire through Ferret's encoded output abstraction:

```go
type Output struct {
	ContentType string
	Content     []byte
}
```

The Wire contract is exactly `content type + encoded bytes`. Wire must not
intercept, expose, reconstruct, or create a parallel representation of internal
Ferret runtime values for normal execution. Ferret intentionally shields hosts
from runtime value implementation details through its encoding layer.

Do not introduce:

* Wire-specific Ferret engine options;
* private codecs that intercept raw runtime values;
* runtime-value type switches that duplicate Ferret semantics;
* alternate execution paths that bypass Ferret's public `Plan`/`Session` APIs.

Debugger APIs are a separate boundary. They may expose structured frames,
variables, references, breakpoints, locations, and stop state only where
Ferret's public debugger API intentionally provides those concepts.

## Host ownership

Wire exposes an engine supplied by the host:

```go
engine := createApplicationEngine()
server, err := wire.NewServer(engine)
err = server.Serve(ctx, listener)
```

Wire borrows the engine and listener. It does not close the engine, construct or
secure a listener, or reconstruct the application's engine configuration.
Custom modules, functions, policies, resources, configuration, and application
state remain host-owned.

Importing Wire must have no side effects. `NewServer` must never listen, bind,
dial, inspect the environment, or implicitly expose an engine.

## Protocol ownership and compatibility

The versioned protobuf API, currently `ferret.wire.v1`, is the canonical Wire
contract. Generated Go types are not the source contract and must never be
hand-edited.

Preserve protobuf compatibility within a released protocol version. Prefer
additive evolution. Never:

* reuse field numbers or reserved names;
* silently change a field's meaning;
* change field types incompatibly;
* remove existing fields or RPCs without deliberate versioning;
* couple protocol messages to private Go implementation structures;
* mirror DAP or LSP structures for a downstream consumer's convenience.

If an incompatible redesign is necessary after release, introduce the
appropriate protocol version instead of weakening `v1` compatibility. Run the
canonical Buf breaking check against the intended base branch for every
protocol change.

Opaque connection, plan, execution, and debug-session IDs are server-issued.
Clients must not infer their structure or use one logical connection's IDs from
another connection.

## Logical connection lifecycle

A Wire connection is a logical client ownership scope represented by the
long-lived `RuntimeService.Connect` stream. It is not a physical HTTP/2 or
socket connection.

```text
Wire connection
├── plans
├── executions
└── debug sessions
```

Resources created through a logical connection belong to it. When the Connect
stream terminates, cleanup must:

1. reject new operations and cancel in-flight creation;
2. wait for in-flight creation to settle;
3. close debug sessions;
4. cancel and release executions;
5. release plans;
6. remove associated state and terminate owned goroutines.

Release is a committed teardown operation. Concurrent callers that observe the
same in-flight release wait for its retained result. After teardown completes,
the ID is stale and must return the relevant structured not-found error. Do not
retain permanent tombstones.

Do not tie logical ownership to physical HTTP/2 connections, grpc-go transport
internals, stats handlers, peer addresses, or socket identity. Do not introduce
leases, TTLs, reconnect tokens, or heartbeats without a concrete requirement
for reconnectable ownership.

## Concurrency and cancellation

Wire is concurrency-sensitive. Every stateful resource must have explicit
ownership, synchronization, cancellation, and termination behavior.

Pay particular attention to:

* logical connections and server shutdown;
* pending, active, and closing resources;
* plan cascades into executions and debug sessions;
* debug state transitions and stale value references;
* unary cancellation combined with logical-session cancellation;
* event ordering, lag, terminal delivery, and stream exit;
* blocked or disconnected consumers;
* panic-safe completion of detached cleanup.

Context cancellation must propagate into Ferret operations wherever Ferret's
public API accepts a context. Inspection and breakpoint operations must not wait
through a resume and then observe state from a later stop. Prefer the explicit
Wire state lock plus Ferret's serialized debugger API; do not add a second
command scheduler.

Use bounded event buffers and non-blocking producers. A slow client must not
block Ferret execution or leak goroutines. Watcher-limit slots remain owned
until the corresponding stream handler exits, including after lag or a terminal
snapshot. Do not create unbounded queues or detached goroutines without a named
owner and deterministic termination.

Cleanup should be deterministic and idempotent internally. User-visible
resource releases become not-found after completed cleanup, as described above.

## Resource limits and security

Treat every Wire server as a potential remote-code-execution boundary, even
when the intended transport is local IPC. Requests and lifecycle identifiers
are untrusted input.

`DefaultServerLimits` is the secure default baseline:

* 64 logical connections;
* 128 plans per connection;
* 128 executions per connection;
* 32 debug sessions per connection;
* 8 watch streams per execution or debug session;
* 256 breakpoints per debug session;
* 4 MiB inbound gRPC messages;
* 4 MiB outbound gRPC messages.

Hosts may replace the complete positive limit set with `WithServerLimits`.
Pending, active, and closing resources all count against the applicable limit.
Do not add limit bypasses for internal convenience.

Mandatory security principles:

* no implicit or externally reachable default listener;
* validate identifiers, required fields, ranges, and request state;
* avoid unbounded allocation from client-controlled sizes or nesting;
* sanitize internal failures and panic values;
* do not leak unnecessary filesystem, environment, transport, or host details;
* preserve Ferret filesystem and network policy instead of bypassing it;
* handle malformed requests without panics or retained state;
* do not trust client-provided resource ownership;
* do not weaken Ferret security boundaries for Wire convenience.

Authentication and TLS policy are host/transport concerns until a separate,
explicit contract is introduced. Unsafe network exposure must never become the
default.

## Generated code

Protobuf definitions and Buf configuration are source. Files under
`gen/ferret/wire/v1` are derived output and exempt from handwritten style rules.
Never hand-edit generated protobuf or gRPC code.

When generator inputs change:

1. run `make generate`;
2. inspect the complete generated diff;
3. commit source and generated changes together;
4. run `make check-generate`.

Generated changes without corresponding protobuf or generation-configuration
changes are suspicious and require explanation.

## Client API

The handwritten `client` package is a thin domain facade over generated gRPC
clients. Its responsibilities are limited to:

* logical Connect lifecycle;
* private ownership and propagation of the logical connection ID;
* ergonomic plan, execution, debug, and event operations;
* explicit parameter conversion;
* structured error mapping;
* hiding unnecessary protobuf and gRPC ceremony.

Do not create a second comprehensive object model merely to hide protobuf.
Introduce handwritten domain types only when they materially improve API
stability, correctness, or ergonomics.

> Hide protocol ceremony, not protocol concepts.

Client-created watch streams belong to the client's logical lifecycle. Reject
new operations as soon as close begins. Closing the facade must not close the
caller-owned `grpc.ClientConnInterface`.

## Public API discipline

Treat the top-level `wire` package, the `client` package, and the versioned
protobuf service as API-sensitive.

* Do not export new symbols unless an external contract requires them.
* Keep logical connection IDs private in the handwritten client.
* Add contract-focused doc comments to necessary exported APIs.
* Preserve encoded-output, host-ownership, and listener-ownership boundaries.
* Keep protocol concepts versioned and explicit.
* Call out every intentional protocol or public API change in the final report.
* Cover deliberately changed edge behavior and the new expected contract.

## Go type and file structure

These rules are mandatory for handwritten Go code:

* Prefer one grouped `type ( ... )` declaration for related package-level types.
* Group structs, interfaces, aliases, and named primitive types when they form a
  cohesive responsibility.
* Do not split files one type at a time merely because types have methods.
* Keep related lifecycle or protocol-adaptation types together when proximity
  improves understanding.
* Split files by responsibility, such as server lifecycle, execution handling,
  debugger commands, debugger inspection, debugger events, or client lifecycle.
* Avoid overloaded files that combine unrelated responsibilities.
* Do not create `helpers.go`, `utils.go`, or similar dumping grounds.

Generated code is exempt.

## Function and method ownership

These rules are mandatory for handwritten Go code:

* Organize files around cohesive responsibilities rather than individual types.
* Keep methods close to the state and lifecycle they own.
* Constructors may live beside the types they construct.
* A type-centered file must not mix in unrelated package-level functions.
* If behavior belongs to a connection, plan, execution, debug session, watcher,
  server, or client lifecycle, prefer a method on that owner.
* Move genuinely package-level behavior into a predictably named,
  responsibility-focused file.
* Do not create arbitrary collections of small helper functions.

## Go control-flow spacing

These rules are mandatory for handwritten Go code. Blank lines separate logical
units and make control transfer visible.

### Producer and immediate check

A declaration, assignment, lookup, call, assertion, or parse operation stays
adjacent to the `if` that immediately validates or consumes it:

```go
response, err := stream.Recv()
if err != nil {
	return err
}

session, ok := sessions[id]
if !ok {
	return ErrNotFound
}
```

Do not insert a blank line between the producer and its immediate check. If the
producer/check unit follows separate logic, add a blank line before it.

### Independent control flow

Separate independent control-flow blocks with a blank line:

```go
if request == nil {
	return ErrInvalidRequest
}

if err := ctx.Err(); err != nil {
	return err
}
```

After a completed control-flow block, add a blank line before a separate
statement or logical unit.

### Return and break

`return` and `break` begin a separate logical group when another statement
precedes them in the same block:

```go
snapshot := execution.snapshot()

return snapshot
```

Do not add an artificial leading blank line at the start of a function or block.

## Comments

Do not mechanically comment every symbol. Comments should explain contracts,
ownership, invariants, or non-obvious decisions, including:

* protobuf compatibility choices;
* logical resource ownership and release semantics;
* cancellation and shutdown behavior;
* lock ordering and concurrency assumptions;
* bounded stream behavior;
* security assumptions and sanitization.

Exported public server and client APIs should have useful contract-focused doc
comments. Avoid comments that merely restate a symbol name or signature.

## Engineering discipline

For every non-trivial change:

1. identify the owning Wire subsystem;
2. identify protocol, API, lifecycle, and security invariants;
3. choose the smallest coherent implementation;
4. add or update contract-focused tests at the owning layer;
5. evaluate concurrency, cancellation, and cleanup;
6. evaluate untrusted-input and resource-exhaustion implications;
7. evaluate protobuf and public API compatibility;
8. run narrow validation first;
9. broaden validation according to risk;
10. update affected documentation;
11. perform the mandatory final self-review;
12. fix findings and rerun affected validation;
13. report actual validation and limitations accurately.

Do not perform opportunistic refactors unrelated to the task. A task is not
complete merely because the first implementation compiles or tests pass.

## Tests and validation

Test behavior at the layer that owns it and add integration coverage when a
contract crosses layers:

| Behavior | Owning test layer |
| --- | --- |
| Protobuf/API compatibility | Buf lint and breaking checks |
| Server request semantics | `internal/grpcserver` tests |
| Logical ownership and limits | `internal/core` lifecycle tests |
| Ferret execution adaptation | Top-level integration tests using public Ferret APIs |
| Cancellation and cleanup | Lifecycle and integration tests |
| Debugger commands and inspection | Core and integration debugger tests |
| Client facade and conversions | `client` contract tests |

Lifecycle-sensitive changes should cover positive behavior and relevant
cancellation, disconnect, shutdown, stale or unknown IDs, double cleanup,
concurrent operations, slow consumers, and panic paths. Use bounded test waits
and deadlines. Check cleanup errors.

Run the race detector for concurrency-sensitive changes. Never claim validation
passed unless the command actually ran successfully.

Canonical repository validation is:

```sh
make fmt
make check-fmt
make generate
make check-generate
make proto-lint
make proto-breaking BUF_BREAKING_AGAINST=.git#branch=main
make check-tidy
make vet
make test
make test-race
make build
```

Use the relevant subset for narrow iteration, then broaden according to risk.
`make generate` is required when generator inputs change.

## Performance

A change is performance-significant when it can materially affect RPC latency,
event throughput, allocations per request or event, serialization, buffering,
synchronization, execution hot paths, resource lookup, cancellation, or
concurrent clients.

For meaningful hot-path changes, run or add a representative benchmark and
compare `ns/op`, `B/op`, and `allocs/op`. Investigate material regressions. Do
not micro-optimize speculative paths at the expense of clarity, security, or
correctness. If the environment cannot run a required benchmark, report that
fact rather than claiming benchmark validation.

## Mandatory final self-review

After implementation and initial validation, inspect the complete final diff.
This is a second-pass review, not a statement that tests passed.

### Correctness

Check incomplete request handling, invalid state transitions, incorrect error
mapping, stale IDs, malformed inputs, unsupported codecs, cancellation, and
terminal event ordering.

### Lifecycle and concurrency

Check goroutine leaks, retained resources, missing cancellation, unsafe
`WaitGroup` use, lock ordering, deadlocks, races, blocked producers, watcher-slot
release, and shutdown ordering.

### Architecture and API

Check for Ferret semantics duplicated in Wire, transport details leaking into
domain APIs, DAP/LSP concerns entering Wire, host engine construction leaking
into Wire, unnecessary protobuf contamination of the client facade, and
unnecessary exported APIs.

### Security

Check unintended listener exposure, missing limits, unbounded client-controlled
allocation, information leakage, trust of client-controlled ownership, panic
sanitization, and Ferret policy bypasses.

### Protocol compatibility

Check field-number or name reuse, incompatible field changes, accidental RPC
changes, missing reservations, generated/source drift, and an incorrect Buf
comparison base.

### Organization and scope

Check unnecessary abstractions, generic helper files, inconsistent ownership,
fragmented related types, overloaded files, comment wallpaper, temporary code,
debugging artifacts, unrelated edits, and scope expansion.

When review finds a meaningful issue, fix it and rerun every affected command.
Do not use self-review to justify speculative redesign or unrelated cleanup.

## CI and documentation synchronization

CI uses the Makefile's canonical targets on Linux, macOS, and Windows. Linux
also runs race detection, protobuf linting, generation consistency checks, and
pull-request Buf breaking checks against the fetched base branch. Keep CI
orchestration in the workflow and command composition in the Makefile.

Documentation is part of implementation. Keep detailed architecture, protocol
design, lifecycle semantics, security considerations, and other design
documentation under `docs/`. Keep `AGENTS.md` concise and use it to reference
the relevant documents rather than duplicating their contents.

Update the appropriate documentation whenever a change affects documented
architecture, protocol behavior, lifecycle semantics, public APIs, security
assumptions, supported transports, or development workflow. Update `README.md`
when the change affects its user-facing overview, setup, or examples, and keep
protobuf comments synchronized with the protocol contract they describe.

Avoid documentation churn for behavior-neutral internal changes. If a public
change requires documentation in another Ferret ecosystem repository that is
not in scope or available, identify the exact follow-up in the final report.

## Scope discipline

Wire is a narrow bridge between Ferret and external tooling. Do not allow it to
become another Ferret runtime, an LSP or DAP implementation, a module registry,
a plugin manager, an application framework, or a distributed execution system.
New responsibilities require a concrete architectural reason and an explicit
contract.

## Final reporting

For non-trivial changes, report concisely:

* owning subsystems and files changed;
* protocol and public API behavior changed or preserved;
* lifecycle, concurrency, and security impact;
* tests added or updated;
* exact validation actually run;
* benchmark results when applicable;
* documentation impact;
* completion of mandatory self-review and corrected findings;
* remaining limitations or skipped validation.
