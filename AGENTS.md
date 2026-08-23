# Task: Establish Wire AGENTS.md

Create the canonical `AGENTS.md` for the Wire repository.

Use the attached Ferret `AGENTS.md` as the **style, engineering-discipline, Go-style, validation, and self-review baseline**, but adapt it specifically to Wire.

Do not simply copy Ferret's file.

Wire has substantially different responsibilities:

```text
Ferret
    ↑
   Wire
    ↑
    ├── ferretd
    ├── CLI
    ├── Lab
    └── host applications
```

Wire is a security-sensitive RPC boundary exposing an application's configured Ferret engine to external tooling.

The resulting `AGENTS.md` should establish high-quality engineering standards from the beginning of the project.

## Sources of truth

Define repository authorities based on the actual current Wire repository.

At minimum inspect and reference where applicable:

- `go.mod` for module path and Go version;
- `Makefile` for canonical development commands;
- protobuf sources for Wire protocol semantics;
- Buf/protobuf generation configuration;
- generated protobuf/gRPC files as derived artifacts only;
- CI workflows for repository validation;
- current implementation and tests for behavior.

Do not invent files or development documentation that does not yet exist.

When generated code disagrees with protobuf source, protobuf source is authoritative.

## Architecture

Document the fundamental dependency invariant:

```text
ferret
   ↑
wire
   ↑
consumers
```

Ferret core must never depend on Wire.

Wire adapts Ferret's public execution and debugger APIs for communication across a process boundary.

Wire must not duplicate or redefine Ferret semantics.

Explicitly establish ownership boundaries such as:

| Concern | Owner |
| --- | --- |
| FQL/runtime semantics | Ferret |
| Engine construction/configuration | host application |
| Output encoding semantics | Ferret |
| Debugger semantics | Ferret |
| Wire protocol | protobuf definitions |
| RPC adaptation | Wire server |
| Logical client lifecycle | Wire |
| gRPC transport | Wire transport/server layer |
| DAP translation | ferretd, not Wire |
| LSP/language intelligence | ferretd/compiler tooling, not Wire |

## Execution boundary

Document this invariant explicitly.

Normal Ferret execution results are exposed through Ferret's existing encoded output abstraction:

```go
type Output struct {
    ContentType string
    Content     []byte
}
```

Wire preserves this abstraction as:

```text
content type + encoded bytes
```

Wire must **not** intercept, expose, reconstruct, or create a parallel representation of internal Ferret runtime values for normal execution.

Ferret intentionally shields hosts from runtime value implementation details through its encoding layer.

Do not introduce:

- Wire-specific Ferret engine options;
- private codecs used to intercept raw values;
- runtime-value type switches duplicating Ferret semantics;
- alternate execution paths bypassing Ferret's public Session API.

Debugger APIs are a separate boundary and may expose structured debugger state where Ferret intentionally provides it.

## Host ownership

Wire must expose an engine supplied by the host.

Conceptually:

```go
engine := createApplicationEngine()

wire.Serve(ctx, engine, ...)
```

Wire must never attempt to reconstruct the application's engine configuration.

Custom:

- modules;
- functions;
- configuration;
- resources;
- application state

remain host-owned.

Importing Wire must have no side effects and must never implicitly expose an engine or start a listener.

## Protocol ownership

The versioned protobuf API is the canonical Wire contract.

For example:

```proto
package ferret.wire.v1;
```

Generated Go bindings are derived artifacts.

Never hand-edit generated protobuf/gRPC code.

Protocol evolution must preserve protobuf compatibility within `v1`.

Prefer additive changes.

Never:

- reuse protobuf field numbers;
- silently change field meaning;
- change field types incompatibly;
- remove existing RPCs/fields without deliberate versioning;
- couple messages to Go implementation structures;
- mirror DAP structures merely for ferretd convenience.

If an incompatible protocol redesign becomes necessary, introduce an appropriate new protocol version rather than weakening `v1` compatibility.

## Logical connection lifecycle

Document the distinction between a **Wire connection** and a physical gRPC connection.

A Wire connection is a logical client ownership scope represented by the long-lived Connect stream.

Conceptually:

```text
Wire connection
├── plans
├── executions
└── debug sessions
```

Resources created through a logical connection belong to it.

When the Connect stream terminates:

- active executions must be cancelled;
- debug sessions must be closed;
- plans must be released;
- associated server state must be removed;
- owned goroutines/resources must terminate.

Do not tie resource ownership to:

- physical HTTP/2 connections;
- grpc-go transport implementation details;
- experimental stats handlers;
- socket identity.

Do not introduce leases, TTLs, or heartbeat machinery unless a concrete future requirement needs reconnectable resource ownership.

## Concurrency and lifecycle

Wire is concurrency-sensitive.

For every stateful resource, ownership and cleanup must be explicit.

Pay particular attention to:

- logical connections;
- plans;
- executions;
- debug sessions;
- event streams;
- cancellation;
- server shutdown;
- client disconnect;
- blocked/slow consumers.

Do not create unbounded event queues.

A slow or disconnected client must not indefinitely block Ferret execution or leak goroutines.

Cleanup should be deterministic and idempotent where practical.

Context cancellation must propagate through Wire into Ferret operations.

Avoid detached goroutines without explicit ownership and termination semantics.

## Security boundary

Treat every Wire server as a potential remote-code-execution boundary.

Even when currently intended for local IPC, requests are untrusted input.

Mandatory principles:

- no implicit listeners;
- no externally reachable default listener;
- validate identifiers and request state;
- enforce sensible message-size/resource limits;
- avoid unbounded allocation from client-controlled values;
- do not leak unnecessary host filesystem/environment/internal details;
- preserve Ferret's own filesystem/network security policies rather than bypassing them;
- handle malformed requests safely;
- do not trust client-provided lifecycle identifiers;
- do not weaken Ferret security boundaries for Wire convenience.

Authentication/TLS policy may evolve separately, but unsafe network exposure must never become the default.

## Generated code

Carry over Ferret's generated-code discipline and adapt it for protobuf.

Protobuf definitions/configuration are source.

Generated Go protobuf/gRPC files are derived output.

Never hand-edit generated files.

When generator inputs change:

1. run the canonical generation command;
2. inspect the generated diff;
3. commit source and generated changes together;
4. run generation verification.

Generated-code changes without corresponding source/configuration changes should be treated as suspicious.

## Client API

Wire may provide a thin handwritten Go facade over generated gRPC clients.

Its responsibilities should remain narrow:

- logical Connect lifecycle;
- ownership/propagation of the logical connection ID;
- ergonomic operation/event APIs;
- structured error mapping;
- hiding unnecessary protobuf/gRPC ceremony.

Do not create a second comprehensive object model merely to hide protobuf.

Introduce handwritten domain types only where they materially improve API stability, correctness, or ergonomics.

Guiding principle:

> Hide protocol ceremony, not protocol concepts.

## Go type/file structure

Carry over the Ferret `AGENTS.md` rules for:

- grouped related package-level type declarations;
- responsibility-based file organization;
- avoiding `helpers.go` / `utils.go`;
- keeping cohesive related types together;
- not creating one-file-per-type fragmentation.

These rules should remain mandatory for handwritten Go code.

Generated code is exempt.

## Function and method ownership

Carry over Ferret's rules around:

- methods for behavior owned by type state/lifecycle;
- constructors being allowed beside owned types;
- avoiding unrelated package-level functions mixed into type-centered files;
- responsibility-focused organization;
- avoiding arbitrary helper-function dumping grounds.

Adapt examples to Wire concepts where useful.

## Control-flow spacing

Carry over Ferret's established Go control-flow spacing conventions exactly.

In particular:

- producer + immediate error/state check stay adjacent;
- independent logical/control-flow blocks are separated;
- `return`/`break` begin a separate logical group when preceded by another statement;
- do not introduce artificial leading blank lines.

Generated code is exempt.

## Comments

Carry over Ferret's comment philosophy.

Do not mechanically comment everything.

Comments should explain:

- protocol invariants;
- lifecycle ownership;
- cancellation semantics;
- concurrency behavior;
- security assumptions;
- non-obvious protobuf compatibility decisions.

Exported public SDK APIs should have useful contract-focused documentation.

Avoid comments that merely restate symbol names.

## Engineering discipline

Adapt Ferret's engineering-discipline section.

For every non-trivial change:

1. identify the owning Wire subsystem;
2. identify protocol/API/lifecycle invariants;
3. choose the smallest coherent implementation;
4. add/update contract-focused tests;
5. evaluate concurrency and cleanup;
6. evaluate security implications;
7. evaluate protobuf compatibility;
8. run narrow validation first;
9. broaden validation according to risk;
10. perform mandatory final self-review;
11. fix findings and rerun affected validation;
12. report actual validation accurately.

Do not perform opportunistic refactors unrelated to the task.

## Tests

Require tests at the layer owning the behavior.

Examples:

- protobuf/API compatibility → protocol checks;
- server request semantics → server tests;
- connection ownership → lifecycle tests;
- execution → integration with Ferret public API;
- cancellation → cancellation/resource cleanup tests;
- debugger behavior → debugger adapter tests;
- client facade → client contract tests.

Lifecycle-sensitive changes should cover positive behavior plus relevant:

- cancellation;
- disconnect;
- shutdown;
- stale/unknown IDs;
- double cleanup;
- concurrent operations;
- slow consumers.

Run the race detector for concurrency-sensitive changes.

Never claim validation passed unless it actually ran.

## Performance

Adapt Ferret's significant-change philosophy to Wire.

Changes are performance-significant when they can materially affect:

- RPC latency;
- event throughput;
- allocations per event/request;
- serialization;
- buffering;
- synchronization;
- execution hot paths;
- connection/resource lookup;
- cancellation;
- concurrent clients.

Benchmark meaningful hot-path changes when appropriate.

Do not micro-optimize speculative paths at the expense of clarity or correctness.

## Mandatory final self-review

Preserve Ferret's strong second-pass self-review requirement.

For every non-trivial task, inspect the **complete final diff** after implementation and initial validation.

Review explicitly for:

### Correctness

- incomplete request handling;
- invalid state transitions;
- incorrect error mapping;
- stale IDs;
- malformed input behavior.

### Lifecycle/concurrency

- goroutine leaks;
- resources surviving connection teardown;
- missing cancellation;
- deadlocks;
- races;
- blocked event producers;
- shutdown ordering.

### Architecture

- Ferret semantics duplicated in Wire;
- transport details leaking into protocol/domain APIs;
- DAP/LSP concerns entering Wire;
- host engine construction leaking into Wire;
- generated protobuf types unnecessarily contaminating SDK APIs;
- unnecessary exported API.

### Security

- unintended listener exposure;
- missing input/resource limits;
- information leakage;
- trust of client-controlled state;
- Ferret security boundaries being bypassed.

### Protocol compatibility

- field-number reuse;
- incompatible message changes;
- accidental RPC removal/change;
- generated code not synchronized with source.

### Organization

- unnecessary abstraction;
- generic helper files;
- inconsistent method/function ownership;
- fragmented related types;
- comment wallpaper;
- temporary/debug code.

When self-review finds a meaningful issue, fix it and rerun affected validation.

A task is not complete merely because tests pass.

## CI expectations

Agents should use the repository's canonical Makefile/CI commands rather than recreating command sequences independently.

At minimum, where available, final validation should cover:

- formatting;
- generation consistency;
- protobuf lint;
- protobuf breaking-change checks;
- Go tests;
- Go vet/static analysis;
- race tests for concurrency-sensitive changes;
- module tidiness.

Use actual repository commands rather than hard-coding commands in this document if the Makefile already owns them.

## Documentation synchronization

Documentation is part of implementation.

Update repository documentation when changing:

- protocol behavior;
- lifecycle semantics;
- public SDK APIs;
- security assumptions;
- supported transports;
- development workflow.

Do not create documentation churn for behavior-neutral implementation changes.

If a change affects another Ferret ecosystem repository's public documentation but that repository is unavailable, explicitly identify the required follow-up.

## Scope discipline

Wire should remain a narrow bridge between Ferret and external tooling.

Do not allow it to gradually become:

- another Ferret runtime;
- an LSP implementation;
- a DAP implementation;
- a module registry;
- a plugin manager;
- an application framework;
- a distributed execution framework.

New responsibilities require a concrete architectural reason.

## Final reporting

For non-trivial changes, report:

- owning subsystem/files changed;
- protocol/API behavior changed or preserved;
- lifecycle/concurrency impact;
- security impact;
- tests added/updated;
- exact validation actually run;
- benchmark results where applicable;
- documentation impact;
- mandatory self-review completion and findings corrected;
- remaining limitations or skipped validation.

Keep reports concise and factual.