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
* Documentation under `docs/` owns detailed architecture, design, lifecycle,
  protocol, and security documentation.
* `README.md` owns the user-facing overview, setup, examples, and development
  entry points.

The current module is `github.com/MontFerret/wire`, uses Go 1.25, and adapts the
Ferret v2 dependency declared in `go.mod`. Verify these values rather than
copying them into implementation logic.

When generated bindings disagree with protobuf source, protobuf source is
authoritative. When descriptive documentation disagrees with current source or
tests, verify the intended contract and correct the stale documentation.

## Architecture

Architecture and design documentation lives under [docs/](docs/). Read
[Wire Architecture](docs/architecture.md) before changing boundaries, protocol
semantics, lifecycle, concurrency, limits, or security, and update it with
affected contracts. Read [Client Handles](docs/client.md) before changing the
handwritten client resource model, and [Client Architecture](docs/client-architecture.md)
before changing its domain, protocol-adapter, or lifecycle boundaries.

Keep detailed rationale in docs rather than this operating guide. Start in the
layer that owns the requested behavior, preserve the dependency direction from
Ferret through Wire to consumers, and do not move DAP, LSP, transport, or
host-configuration semantics into Wire for convenience.

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

## Client and public APIs

The top-level wire package, the client package, and the versioned protobuf
service are API-sensitive. Follow [Client Handles](docs/client.md) for the
facade ownership and lifecycle contract.

Export only externally required symbols, keep logical connection and resource
IDs private in the handwritten client, add contract-focused comments, and
preserve encoded-output, host-engine, listener, and caller-owned transport
boundaries. Call out intentional protocol or public API changes and cover their
edge behavior with tests.

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

## Clean code and design

Apply clean-code and SOLID principles pragmatically. Prefer cohesive types with
one clear responsibility, keep behavior with the domain object that owns it,
and compose narrow collaborators instead of broad manager or facade types.
Avoid god objects and anemic domain models. Introduce interfaces and
abstractions only for a concrete responsibility, substitution point, or useful
test seam; do not mechanically reproduce design patterns. Construct valid
objects up front, encapsulate implementation details, keep dependency surfaces
narrow, and favor code that can be understood and changed locally.

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
