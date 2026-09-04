# Wire v1 Protocol

`ferret.wire.v1` is the pre-stable remote projection of the Unified API. The
protobuf sources under `proto/ferret/wire/v1` are normative; this document
records why every retained symbol belongs at this boundary and why removed
symbols do not.

## Classification taxonomy

| Class | Meaning |
| --- | --- |
| A | Unified semantic: a portable concept owned by `github.com/MontFerret/api`. |
| B | Wire resource concern: logical identity, ownership, lifecycle, limits, or replay needed only because the API is remote. |
| C | Transport concern: protobuf/gRPC framing, status details, encoded parameter representation, or sequencing. |
| D | Implementation leakage: a Ferret-native or host-specific concept that must not appear in Wire. |
| E | Historical artifact: a redundant or superseded v1 shape retained only as a reservation or breaking-change record. |

Only A-C concepts exist in the final schema. D and E concepts are listed under
[Removed surface](#removed-surface).

## Ownership and lifecycle

One `Connect` stream creates a logical connection independently of the physical
HTTP/2 connection. The server sends one handshake and keeps the stream open as
the connection lifetime signal. Several logical connections may share one
transport, but their IDs and resources cannot be mixed.

```text
Connection
├── direct Runtime Execution
└── reusable Plan
    ├── direct Execution
    ├── durable Session
    │   └── one active Execution
    └── DebugSession
        └── debugger.Session
```

IDs are opaque, server-issued, and scoped to the owning connection. Releasing a
resource makes its ID stale. Releasing a normal Session rejects new runs,
settles its active Execution, and closes its hosted `api.Session`. Releasing a
Plan settles direct executions, normal sessions, and debug sessions before
closing the Plan. Closing a connection or losing its Connect
stream first prevents new children, waits for in-flight creation, then settles
executions, normal sessions, debug sessions, and plans. Wire borrows the configured `api.Runtime`
and listener and never closes either.

`Compile` and `CompileDebug` create reusable Plans. Each `Execute` creates a new
temporary `api.Session`. `CreateSession` instead constructs one durable
`api.Session`; each sequential `RunSession` creates a distinct Execution that
invokes the same hosted session. `RunRuntime` creates a connection-owned
Execution that calls the borrowed `api.Runtime.Run` directly. Each
`CreateDebugSession` creates a new `debugger.Session` from a debug Plan.
Cancellation or `Terminate` changes runtime state but does not release the
remote resource. The corresponding Release RPCs are the ownership operations.

## State, watches, and debugger references

Execution creation publishes a running snapshot. Debug-session creation
publishes a created snapshot. A fresh watch always replays the latest created,
running, stopped, or terminal snapshot, then observes ordered changes. Sequence
numbers are positive and monotonic within one resource history. A debug event
also carries a kind because start and continue both produce running snapshots.

A completed snapshot has encoded output. A failed snapshot has `Failure` and
may retain partial encoded output. A cancelled or terminated snapshot has no
failure. A stopped debug snapshot carries its stop reason, range, depth, and
breakpoint hits when applicable; a runtime-error stop may also carry a failure.

Watches are non-owning. Cancelling or disconnecting one watch releases only
that subscription and never cancels the execution or debugger. Producers do
not block on consumers. Watcher counts and buffers are finite; a slow watcher
is detached with `ResourceExhausted`. Terminal state remains replayable until
explicit release, so no redundant Get RPC is needed.

Debugger value reference zero means no expandable value and is invalid as a
`Variables` request. A positive reference is scoped to the current stopped
state of one debug session and becomes stale on resume. Until the Unified API
defines a portable typed not-found error, a stale positive reference is
reported as `InvalidState`, not as a fabricated reference-not-found category.

## Sources, parameters, output, and diagnostics

`Source.name` and `Location.source_name` are semantic source names, not
filesystem paths. Lines are one-based, columns are zero-based, and span offsets
are non-negative producer-defined units. Wire preserves source content, names,
ranges, and spans without interpreting the source name or span unit.

Parameters are limited to null, bool, signed `int64`, finite protobuf `double`,
string, bytes, arrays, and string-keyed objects, nested to the configured Wire
limit. Integer and floating-point representations remain distinct. Protobuf
JSON renders 64-bit integers as decimal strings and finite doubles as JSON
numbers; although protobuf JSON has spellings for `NaN` and infinities, Wire
rejects those values on both client encoding and server decoding.

Execution and debugger completion preserve Unified API encoded output exactly:
`content_type` plus bytes. Wire never interprets the bytes as runtime values.

The handwritten Go adapters project these messages onto the public semantic
packages `pkg/execution`, `pkg/debugger`, and `pkg/failure`. These values are
shared by client and server and deliberately omit protocol resource IDs. The
adapters copy mutable output, diagnostic, range, and breakpoint data at
ownership and delivery boundaries and validate every state, event-kind, and
failure-category enum explicitly in both directions.

Only typed `diagnostics.Diagnostics` values cross the boundary. Wire never
parses runtime error strings. Each diagnostic preserves kind, message, source
name and content, ordered annotations, primary markers, ranges, hint, and note.
Immediate errors carry `DiagnosticSet` as a separate gRPC status detail beside
`ErrorDetail`. Asynchronous failures carry it in `Failure.diagnostic_set`.
Summary messages remain sanitized, and arbitrary causes, panic values, stacks,
and implementation error text never cross the protocol.

## Complete retained-surface audit

The following tables cover every retained service, RPC, message, enum, oneof,
and field. Rows that list several fields classify each listed field.

### Runtime and common failure data

| Symbol | Class | Fields or contract |
| --- | --- | --- |
| service `RuntimeService` | B/C | Logical connection lifecycle over gRPC. |
| RPC `Connect` | B/C | `ConnectRequest` to streaming `ConnectResponse`; creates and signals one logical connection. |
| RPC `CloseConnection` | B/C | Explicit connection teardown and empty acknowledgement. |
| `ConnectionId` | B | `value=1`, opaque and non-empty. |
| `ProtocolInfo` | C | `name=1`, `version=2`; identifies Wire, not the runtime. |
| `RuntimeIdentity` | C | Host-supplied `name=1`, optional `version=2`, optional `instance_id=3`. |
| `ConnectRequest` | C | Empty request. |
| `ConnectResponse` | B/C | `connection_id=3`, `protocol=4`, optional `runtime_identity=5`; sent once. |
| `CloseConnectionRequest` | B/C | `connection_id=1`. |
| `CloseConnectionResponse` | C | Empty acknowledgement. |
| enum `ErrorCategory` | A/B/C | `UNSPECIFIED=0`; compilation `2`, execution `3`; Plan `4`, Execution `5`, DebugSession `6`, Connection `7`, Session `16` not found; invalid state `8`; internal runtime boundary `10`; watcher lag `11`; breakpoint not found `15`. |
| `ErrorDetail` | C | `category=1`; separate gRPC status detail for non-native Wire categories. |
| `DiagnosticAnnotation` | A/C | `range=1`, `message=2`, `primary=3`. |
| `Diagnostic` | A/C | `kind=1`, `message=2`, `hint=3`, `note=4`, `source=7`, ordered `annotations=8`. |
| `DiagnosticSet` | A/C | Ordered `diagnostics=1`; transport wrapper for `diagnostics.Diagnostics`. |
| `Output` | A/C | `content_type=1`, encoded `content=2`. |
| `Failure` | A/C | `category=1`, sanitized `message=2`, optional `diagnostic_set=4`. |

### Source model

| Symbol | Class | Fields or contract |
| --- | --- | --- |
| `Source` | A/C | Required `content=1`, semantic `name=3`. |
| `Position` | A/C | One-based `line=1`, zero-based `column=2`. |
| `Span` | A/C | Producer-defined `start=1`, `end=2`, with `0 <= start <= end`. |
| `Location` | A/C | Semantic `source_name=1`, required `position=2`. |
| `Range` | A/C | Required `location=1`, `span=2`. |

### Plans

| Symbol | Class | Fields or contract |
| --- | --- | --- |
| service `PlanService` | A/B/C | Remote creation and ownership of reusable Unified API plans. |
| RPC `Compile` | A/B/C | Creates a normal Plan from `CompileRequest`; returns `CompileResponse`. |
| RPC `CompileDebug` | A/B/C | Creates a debug Plan from `CompileDebugRequest`; returns `CompileDebugResponse`. |
| RPC `ReleasePlan` | B/C | Releases the Plan and descendants. |
| `PlanId` | B | `value=1`, scoped to the owning connection. |
| enum `OptimizationLevel` | A/C | `UNSPECIFIED=0`, `NONE=1`, `BASIC=2`, `FULL=3`, `AGGRESSIVE=4`; unspecified preserves runtime default. |
| `CompileOptions` | A/C | `optimization_level=2`. |
| `Plan` | A/B/C | `id=1`, ordered parameter names `parameters=2`. |
| `CompileRequest` | A/B/C | `connection_id=1`, `source=2`, optional `options=3`. |
| `CompileResponse` | B/C | Required `plan=1`; compile diagnostics use status details. |
| `CompileDebugRequest` | A/B/C | `connection_id=1`, `source=2`, optional `options=3`. |
| `CompileDebugResponse` | B/C | Required `plan=1`. |
| `ReleasePlanRequest` | B/C | `connection_id=1`, `plan_id=2`. |
| `ReleasePlanResponse` | C | Empty acknowledgement. |

### Parameter values

| Symbol | Class | Fields or contract |
| --- | --- | --- |
| `Value` | C | Oneof `value` contains exactly one portable representation. |
| oneof `Value.value` | C | `null_value=1`, `boolean_value=2`, signed `integer_value=3`, finite `float_value=4`, `string_value=5`, `binary_value=6`, `array_value=10`, `object_value=11`. |
| `ArrayValue` | C | Ordered `values=1`. |
| `ObjectValue` | C | String-keyed map `fields=1`; empty keys are invalid. |
| `Parameters` | C | Non-empty names mapped through `values=1`. |

### Executions

| Symbol | Class | Fields or contract |
| --- | --- | --- |
| service `ExecutionService` | A/B/C | Creates, controls, releases, and watches asynchronous operations. |
| RPC `Execute` | A/B/C | Creates one asynchronous session and immediately returns its running snapshot. |
| RPC `RunSession` | A/B/C | Creates one Execution for a durable Session run; overlapping runs are invalid state. |
| RPC `RunRuntime` | A/B/C | Creates one connection-owned Execution that invokes hosted `api.Runtime.Run`. |
| RPC `CancelExecution` | A/B/C | Requests cancellation; does not release. |
| RPC `ReleaseExecution` | B/C | Commits cancellation and cleanup. |
| RPC `WatchExecution` | B/C | Non-owning ordered snapshot stream. |
| `ExecutionId` | B | `value=1`, scoped to the owning connection. |
| enum `ExecutionState` | A/C | `UNSPECIFIED=0`, `RUNNING=1`, `COMPLETED=2`, `FAILED=3`, `CANCELLED=4`. |
| `Execution` | A/B/C | `id=1`, discriminator `state=3`, optional encoded `output=4`, optional `failure=5`. |
| `WatchExecutionResponse` | B/C | Positive monotonic `sequence=2`, complete `execution=8`. |
| `ExecuteRequest` | A/B/C | `connection_id=1`, `plan_id=2`, optional `parameters=3`, optional requested `output_content_type=4`. |
| `ExecuteResponse` | B/C | Required running `execution=1`. |
| `RunSessionRequest` | A/B/C | `connection_id=1`, `session_id=2`. |
| `RunSessionResponse` | B/C | Required running `execution=1`. |
| `RunRuntimeRequest` | A/B/C | `connection_id=1`, `source=2`, optional `parameters=3`, optional requested `output_content_type=4`. |
| `RunRuntimeResponse` | B/C | Required running `execution=1`. |
| `CancelExecutionRequest` | B/C | `connection_id=1`, `execution_id=2`. |
| `CancelExecutionResponse` | C | Empty acknowledgement. |
| `ReleaseExecutionRequest` | B/C | `connection_id=1`, `execution_id=2`. |
| `ReleaseExecutionResponse` | C | Empty acknowledgement. |
| `WatchExecutionRequest` | B/C | `connection_id=1`, `execution_id=2`. |

### Normal sessions

| Symbol | Class | Fields or contract |
| --- | --- | --- |
| service `SessionService` | A/B/C | Creates and releases durable normal Unified API sessions. |
| RPC `CreateSession` | A/B/C | Calls `api.Plan.NewSession` once and retains the result. |
| RPC `ReleaseSession` | B/C | Rejects new runs, settles child Executions, then closes the hosted Session. |
| `SessionId` | B | `value=1`, opaque and scoped to the owning connection. |
| `Session` | A/B/C | `id=1`. |
| `CreateSessionRequest` | A/B/C | `connection_id=1`, `plan_id=2`, optional `parameters=3`, optional requested `output_content_type=4`. |
| `CreateSessionResponse` | B/C | Required `session=1`. |
| `ReleaseSessionRequest` | B/C | `connection_id=1`, `session_id=2`. |
| `ReleaseSessionResponse` | C | Empty acknowledgement. |

### Debugger

| Symbol | Class | Fields or contract |
| --- | --- | --- |
| service `DebugService` | A/B/C | Creates, controls, inspects, releases, and watches debugger sessions. |
| RPC `CreateDebugSession` | A/B/C | Creates one debugger session from a reusable debug Plan. |
| RPCs `Start`, `Continue`, `Pause` | A/B/C | Unified debugger commands with direct connection/session addressing and empty responses. |
| RPCs `StepOver`, `StepIn`, `StepOut` | A/B/C | Canonical Unified debugger stepping commands; no compatibility RPCs. |
| RPCs `SetBreakpoint`, `DeleteBreakpoint` | A/B/C | Create or delete debugger-owned breakpoints. |
| RPCs `Frames`, `FrameLocals`, `Variables`, `EvaluateFrame` | A/B/C | Stopped-state inspection; frame slice order supplies the frame index. |
| RPC `Terminate` | A/B/C | Terminates execution but retains the remote session. |
| RPC `ReleaseDebugSession` | B/C | Terminates if needed and releases ownership. |
| RPC `WatchDebug` | B/C | Non-owning ordered snapshot stream with created-state replay. |
| `DebugSessionId` | B | `value=1`, scoped to the owning connection. |
| enum `DebugState` | A/C | `UNSPECIFIED=0`, `CREATED=1`, `RUNNING=2`, `STOPPED=3`, `COMPLETED=4`, `FAILED=5`, `TERMINATED=6`. |
| enum `DebugStopReason` | A/C | `UNSPECIFIED=0`, `ENTRY=1`, `BREAKPOINT=2`, `STEP=3`, `PAUSE=4`, `RUNTIME_ERROR=5`. |
| enum `DebugEventKind` | B/C | `UNSPECIFIED=0`, `STARTED=1`, `CONTINUED=2`, `STOPPED=3`, `COMPLETED=4`, `FAILED=5`, `TERMINATED=6`, `CREATED=7`. |
| enum `BreakpointBindingMode` | A/C | `UNSPECIFIED=0`, `NEXT_EXECUTABLE_IN_SOURCE=1`, `EXACT=2`, `NEXT_EXECUTABLE_IN_FUNCTION=3`. |
| `DebugSession` | A/B/C | `id=1`, state `3`, stop reason `4`, ordered hit IDs `6`, completed output `7`, failed/runtime-error failure `8`, stopped range `9`, depth `10`. |
| `Breakpoint` | A/C | ID `1`, requested location `8`, optional resolved range `9`, point ID `10`, function ID `11`, binding mode `12`, bound flag `13`. |
| `DebugValue` | A/C | Runtime type `1`, display text `2`, stopped-state-scoped reference `3`. |
| `Variable` | A/C | Name `1`, value `2`, mutable flag `3`, parameter flag `4`. |
| `Frame` | A/C | Name `2`, function ID `4`, location `5`; list order is the index. |
| `CreateDebugSessionRequest` | A/B/C | `connection_id=1`, `plan_id=2`, optional `parameters=3`, optional requested `output_content_type=4`. |
| `CreateDebugSessionResponse` | B/C | Required created `session=1`. |
| `StartRequest`, `TerminateRequest` | B/C | `connection_id=1`, `debug_session_id=2`. |
| `StartResponse`, `TerminateResponse` | C | Empty acknowledgements. |
| `ContinueRequest`, `PauseRequest`, `StepOverRequest`, `StepInRequest`, `StepOutRequest` | B/C | `connection_id=2`, `debug_session_id=3`; old command field `1` is reserved. |
| `ContinueResponse`, `PauseResponse`, `StepOverResponse`, `StepInResponse`, `StepOutResponse` | C | Empty; old session snapshot field `1` is reserved. |
| `BreakpointOptions` | A/C | `binding_mode=1`; absent/unspecified uses the Unified API default. |
| `SetBreakpointRequest` | A/B/C | `connection_id=1`, `debug_session_id=2`, `location=4`, optional `options=5`. |
| `SetBreakpointResponse` | A/C | Complete `breakpoint=1`. |
| `DeleteBreakpointRequest` | A/B/C | `connection_id=1`, `debug_session_id=2`, positive `breakpoint_id=3`. |
| `DeleteBreakpointResponse` | C | Empty acknowledgement. |
| `FramesRequest` | B/C | `connection_id=1`, `debug_session_id=2`. |
| `FramesResponse` | A/C | Ordered `frames=1`. |
| `FrameLocalsRequest` | A/B/C | `connection_id=1`, `debug_session_id=2`, zero-based `frame_index=3`. |
| `FrameLocalsResponse` | A/C | Ordered `variables=1`. |
| `VariablesRequest` | A/B/C | `connection_id=1`, `debug_session_id=2`, positive stopped-state `reference=3`. |
| `VariablesResponse` | A/C | Ordered `variables=1`. |
| `EvaluateFrameRequest` | A/B/C | `connection_id=1`, `debug_session_id=2`, zero-based `frame_index=3`, `expression=4`. |
| `EvaluateFrameResponse` | A/C | Required `value=1`. |
| `ReleaseDebugSessionRequest` | B/C | `connection_id=1`, `debug_session_id=2`. |
| `ReleaseDebugSessionResponse` | C | Empty acknowledgement. |
| `WatchDebugRequest` | B/C | `connection_id=1`, `debug_session_id=2`. |
| `WatchDebugResponse` | B/C | Positive monotonic `sequence=2`, event `kind=10`, complete `session=11`. |

## Removed surface

### D: implementation leakage

- Ferret version, module-build metadata, runtime module inventories, native
  runtime values, native diagnostic spans/source identities, panic data, and
  arbitrary implementation errors are absent.
- `Capability`, `RuntimeInfo`, `ConnectionOpened`, `ResourceKind`, and
  `DiagnosticSpan` messages/enums are absent.
- Wire does not expose runtime construction, host policies, DAP/LSP concepts,
  listener security, or bytecode nodes. Direct Runtime execution is an
  asynchronous Wire resource rather than a shortcut on `RuntimeService`.

### E: historical artifacts

- `Location.file` was renamed in place to `source_name=1`; the name `file` is
  reserved. Breakpoint enum value 1 is now
  `NEXT_EXECUTABLE_IN_SOURCE`; the old file-specific enum name is reserved.
- Debug RPCs/messages `Next`, `Step`, and `Out` were replaced by `StepOver`,
  `StepIn`, and `StepOut`. RPC and message names cannot be reserved, so
  descriptor tests enforce their absence.
- `VALUE_REFERENCE_NOT_FOUND=13` is removed; its number and name are reserved.
- `Diagnostic` retains compatible fields 1-4, reserves native fields 5/6 and
  names `source_identity`/`spans`, and uses canonical source/annotations at 7/8.
  `Failure` reserves old field 3/name `diagnostics` and uses
  `diagnostic_set=4`. `ErrorDetail` keeps its old diagnostic slot reserved.
- Runtime reservations: Connect response fields 1/2 and names
  `opened`/`closing`; Connect request field 1/name `client_identity`;
  ErrorCategory 1/9/12/13/14 and their removed names; ErrorDetail fields 2-5
  and names `message`/`resource`/`resource_id`/`diagnostics`.
- Source/plan reservations: Source field 2/name `identity`; CompileOptions field
  1/name `debuggable`; Plan field 3/name `debuggable`; CompileResponse field
  2/name `diagnostics`.
- Value reservations: fields 7/8/9 and names `none_value`/`duration_nanos`/
  `datetime_value`/`regexp_value`.
- Execution reservations: parent Plan field 2/name `plan_id`; old watch fields
  1 and 3-7 plus transition names; cancellation-response field 1/name
  `execution`; removed transition wrapper messages.
- Debug reservations: parent Plan fields 2/5 and name `plan_id`; flat
  breakpoint fields 2-7 and names; Frame fields 1/3 and name `index`;
  SetBreakpoint field 3; old command/session fields; old watch fields 1 and
  3-9 plus transition names; removed transition wrappers, `SourceLocation`,
  `DebugCommand`, `OpenDebugSession`, `StartDebug`, and `StopDebug`.

## Unified API gaps and deferred work

Wire does not invent diagnostic severity, a general structured runtime-error
taxonomy, a Unified API declaration of accepted parameter values, runtime
introspection/versioning, or capability negotiation. Host-supplied
`RuntimeIdentity` is not presented as API introspection.

A broad lower-level client redesign, native Ferret consumer migration, bytecode/node
protocols, distributed execution, and advanced negotiated capabilities remain
separate work. The Universal API adapter deliberately composes the same Wire
resources and does not add reconnection, leases, or transport construction.
