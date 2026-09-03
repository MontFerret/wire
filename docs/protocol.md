# Wire Protocol

`ferret.wire.v1` is the remote projection of the Unified API runtime, plan,
session, and debugger contracts. This document describes the intentional
pre-stable v1 reset. Protobuf sources under `proto/ferret/wire/v1` remain the
normative definition.

## Classification

The audit uses five classifications:

| Class | Meaning |
| --- | --- |
| A | Kept: the contract remains materially unchanged. |
| B | Changed: added, renamed, reshaped, or moved to align with the Unified API. |
| C | Removed: native, redundant, or implementation-specific v1 surface removed with reservations where protobuf fields or enum values existed. |
| D | Unified API gap: no portable upstream contract exists to expose. |
| E | Deferred: intentionally outside this reset. |

Every current RPC, message, and enum is listed below as A or B. Removed v1
symbols are listed as C. Classes D and E record non-schema gaps and future
work; they must not be represented by fabricated protocol data.

## Lifecycle and ownership

`Connect` creates a logical connection and sends one handshake containing its
opaque ID, the Wire protocol name and version, and optional host-supplied
runtime identity. The stream then remains open as the connection's lifetime
signal. A logical connection is independent of its socket or HTTP/2 transport.

Resources form one ownership tree:

```text
Connection
└── Plan
    ├── Execution
    └── DebugSession
```

IDs are server-issued, opaque, and scoped to their owning connection. A client
cannot use an ID through another connection. Explicit release invalidates the
released ID. Releasing a Plan settles its executions and debug sessions before
the Plan. Closing a connection, ending its Connect stream, or shutting down the
server settles pending creation and then releases executions, debug sessions,
and plans in that order. Wire borrows the host's runtime and listener and never
closes either one.

`Compile` and `CompileDebug` both create reusable Plan resources. Their only
public data is the opaque ID and declared parameter names; the creating
operation privately determines whether debug sessions are valid. An absent or
unspecified optimization level passes no plan option and preserves the runtime
default. Explicit none, basic, full, and aggressive values map to the matching
Unified API plan option.

`Execute` creates exactly one `api.Session`, publishes its running snapshot,
and returns without waiting for completion. The Plan remains reusable for
independent executions. Cancellation only requests cancellation; its response
is empty and the resulting state is observed through `WatchExecution`.
`ReleaseExecution` commits cancellation and cleanup.

`CreateDebugSession` creates one debugger session from a debug Plan. Start,
Continue, Pause, Next, Step, Out, and Terminate use the same direct
connection/session address and return empty responses. Terminate does not
release the session; `ReleaseDebugSession` does. Inspection operations require
the appropriate stopped state. The order of `Frame` messages is the zero-based
index accepted by `FrameLocals` and `EvaluateFrame`.

## Watch semantics

Execution and debug watches are independent subscriptions to resource state.
Each watch first receives the latest published snapshot, if one exists, then
strictly ordered changes through one terminal snapshot. This makes terminal
state reconnectable until explicit release. Sequence numbers are local to the
resource event history.

Cancelling or disconnecting one watch releases only that subscription; it does
not cancel the execution or debugger. Producers never block on watchers.
Buffers and watcher counts are finite, and a slow watcher is detached with
gRPC `ResourceExhausted`. Its slot remains accounted for until the stream
handler exits. Releasing a resource invalidates future watches.

`WatchExecutionResponse` contains sequence plus the current Execution. The
Execution state is its lifecycle discriminator. `WatchDebugResponse` also
contains `DebugEventKind`, because started and continued transitions both have
the running state.

## Data and failures

`Source`, `Position`, `Span`, `Location`, and `Range` form one shared source
model. Lines are positive, columns are non-negative, span endpoints are
non-negative, and an end cannot precede its start. Wire validates these values
but does not interpret span units.

Parameters are recursively limited to null, bool, signed int64, double,
string, bytes, arrays, and string-keyed objects. Duration, datetime, regexp,
and custom runtime values are not portable Wire parameters.

Execution and debugger completion share `Output { content_type, content }`.
The bytes are already encoded by the runtime; Wire preserves both fields and
never decodes or transforms the content.

Malformed requests, caller cancellation, and finite-limit violations use
ordinary gRPC `InvalidArgument`, `Canceled`, and `ResourceExhausted` status.
`ErrorDetail` adds only categories whose meaning is specific to the Wire
lifecycle or runtime boundary. Asynchronous execution and debug failures use a
minimal sanitized `Failure { category, message }`. Neither form includes
duplicated messages, resource IDs, native diagnostics, or implementation
causes.

## RPC, message, and enum audit

### Runtime

| Kind | Symbol | Class | Contract |
| --- | --- | --- | --- |
| RPC | `Connect` | B | Streaming logical lifetime with a flat one-shot handshake. |
| RPC | `CloseConnection` | A | Explicit deterministic connection teardown. |
| Message | `ConnectionId` | A | Opaque logical connection ID. |
| Message | `ProtocolInfo` | B | Wire protocol name and version. |
| Message | `RuntimeIdentity` | A | Optional host-supplied identity. |
| Message | `ConnectRequest` | B | Empty; removed client identity is reserved. |
| Message | `ConnectResponse` | B | Direct connection, protocol, and optional runtime identity fields. |
| Message | `CloseConnectionRequest`, `CloseConnectionResponse` | A | Address and acknowledge connection teardown. |
| Enum | `ErrorCategory` | B | Only meaningful Wire-specific categories remain; native gRPC categories are reserved. |
| Message | `ErrorDetail` | B | Category only; duplicated message, resource metadata, and diagnostics are reserved. |
| Message | `Output` | B | Shared encoded execution/debug output. |
| Message | `Failure` | B | Shared minimal asynchronous terminal failure. |

### Source and plans

| Kind | Symbol | Class | Contract |
| --- | --- | --- | --- |
| Message | `Source` | B | Source name and content. |
| Message | `Position`, `Span`, `Location`, `Range` | B | Cohesive source/debugger coordinates and spans. |
| RPC | `Compile` | B | Creates a normal reusable Plan. |
| RPC | `CompileDebug` | B | Creates a debug reusable Plan. |
| RPC | `ReleasePlan` | A | Releases a Plan and descendants. |
| Message | `PlanId` | A | Opaque Plan ID. |
| Enum | `OptimizationLevel` | B | Optional none/basic/full/aggressive plan optimization. |
| Message | `CompileOptions` | B | Optimization only; old debuggable field is reserved. |
| Message | `Plan` | B | ID and declared parameters only. |
| Message | `CompileRequest`, `CompileResponse` | B | Normal source/options request and Plan response; diagnostics are reserved. |
| Message | `CompileDebugRequest`, `CompileDebugResponse` | B | Debug source/options request and the same Plan response. |
| Message | `ReleasePlanRequest`, `ReleasePlanResponse` | A | Address and acknowledge Plan release. |

### Values

| Kind | Symbol | Class | Contract |
| --- | --- | --- | --- |
| Message | `Value` | B | Portable recursive subset; removed duration/datetime/regexp variants are reserved. |
| Message | `ArrayValue`, `ObjectValue`, `Parameters` | A | Recursive arrays, string-keyed objects, and named parameters. |

### Execution

| Kind | Symbol | Class | Contract |
| --- | --- | --- | --- |
| RPC | `Execute` | A | Creates and immediately publishes one asynchronous session. |
| RPC | `CancelExecution` | B | Empty command response; state is observed through Watch. |
| RPC | `ReleaseExecution` | A | Commits cancellation and cleanup. |
| RPC | `WatchExecution` | B | Ordered sequence plus one current snapshot. |
| Message | `ExecutionId` | A | Opaque Execution ID. |
| Enum | `ExecutionState` | A | Running, completed, failed, or cancelled lifecycle. |
| Message | `Execution` | B | ID, state, output, and failure; parent Plan ID is reserved. |
| Message | `WatchExecutionResponse` | B | Sequence and Execution without transition wrappers or echoed ID. |
| Message | `ExecuteRequest`, `ExecuteResponse` | A | Plan, parameters, codec request and immediate Execution snapshot. |
| Message | `CancelExecutionRequest`, `CancelExecutionResponse` | B | Direct address and empty acknowledgement; old snapshot field is reserved. |
| Message | `ReleaseExecutionRequest`, `ReleaseExecutionResponse` | A | Address and acknowledge Execution release. |
| Message | `WatchExecutionRequest` | A | Direct connection/execution address. |

### Debugger

| Kind | Symbol | Class | Contract |
| --- | --- | --- | --- |
| RPC | `CreateDebugSession` | B | Renamed creation of a debugger session. |
| RPC | `Start`, `Continue`, `Pause`, `Next`, `Step`, `Out` | B | Direct minimal address and empty command response. |
| RPC | `Terminate` | B | Renamed termination without release. |
| RPC | `SetBreakpoint` | B | Location plus optional binding behavior. |
| RPC | `DeleteBreakpoint` | A | Deletes one breakpoint ID. |
| RPC | `Frames`, `FrameLocals`, `Variables`, `EvaluateFrame` | B | Complete Unified debugger inspection data with ordered frame indexing. |
| RPC | `ReleaseDebugSession` | A | Commits debugger cleanup. |
| RPC | `WatchDebug` | B | Sequence, event kind, and one DebugSession snapshot. |
| Message | `DebugSessionId` | A | Opaque DebugSession ID. |
| Enum | `DebugState`, `DebugStopReason` | A | Unified debugger lifecycle and stop reason. |
| Enum | `DebugEventKind`, `BreakpointBindingMode` | B | Distinct running transitions and breakpoint binding behavior. |
| Message | `DebugSession` | B | Full state, range, depth, hits, output, and failure; parent Plan ID is reserved. |
| Message | `Breakpoint` | B | Requested location, resolved range/span, point/function IDs, binding mode, and bound state. |
| Message | `DebugValue`, `Variable` | A | Display/reference value and variable flags. |
| Message | `Frame` | B | Name, location, and function ID; transmitted index is reserved. |
| Message | `CreateDebugSessionRequest`, `CreateDebugSessionResponse` | B | Renamed Plan/parameters/codec creation envelope and initial snapshot. |
| Message | `StartRequest`, `StartResponse`, `ContinueRequest`, `ContinueResponse`, `PauseRequest`, `PauseResponse`, `NextRequest`, `NextResponse`, `StepRequest`, `StepResponse`, `OutRequest`, `OutResponse`, `TerminateRequest`, `TerminateResponse` | B | Uniform direct session commands with empty responses. |
| Message | `BreakpointOptions`, `SetBreakpointRequest`, `SetBreakpointResponse` | B | Binding options and complete breakpoint result. |
| Message | `DeleteBreakpointRequest`, `DeleteBreakpointResponse` | A | Address breakpoint deletion and acknowledge it. |
| Message | `FramesRequest`, `FramesResponse`, `FrameLocalsRequest`, `FrameLocalsResponse`, `VariablesRequest`, `VariablesResponse`, `EvaluateFrameRequest`, `EvaluateFrameResponse` | B | Inspection requests and complete Unified values. |
| Message | `ReleaseDebugSessionRequest`, `ReleaseDebugSessionResponse` | A | Address and acknowledge debugger release. |
| Message | `WatchDebugRequest`, `WatchDebugResponse` | B | Direct address and ordered kind/snapshot event. |

## Removed v1 surface

The following class C surface is intentionally absent:

- runtime `Capability`, `RuntimeInfo`, `ConnectionOpened`, `ResourceKind`,
  `DiagnosticSpan`, and `Diagnostic`, including hard-coded capabilities,
  `ferret_version`, module-build data, native diagnostics, resource IDs, and
  duplicated error messages;
- Plan `debuggable` and compile diagnostics;
- parameter duration, datetime, and regexp variants;
- Execution `plan_id`, `ExecutionStarted`, `ExecutionCompleted`,
  `ExecutionFailed`, `ExecutionCancelled`, watch oneofs, echoed execution IDs,
  and the cancellation response snapshot;
- Debug `plan_id`, `SourceLocation`, `DebugCommand`, transmitted frame index,
  flat breakpoint coordinates/verification, transition wrapper messages
  `DebugStarted`, `DebugContinued`, `DebugStopped`, `DebugCompleted`,
  `DebugFailed`, and `DebugTerminated`;
- RPCs `OpenDebugSession`, `StartDebug`, and `StopDebug`, replaced respectively
  by `CreateDebugSession`, `Start`, and `Terminate`;
- direct exposure of `api.Runtime.Run`; clients compose Compile, Execute, Watch,
  and Release.

Removed protobuf fields, names, and enum values are reserved in their owning
messages or enums. Removed RPC and message names are covered by the intentional
pre-stable breaking-change report.

## Unified API gaps

Class D gaps are recorded rather than filled with Wire-specific assumptions:

- no portable diagnostics or runtime error taxonomy;
- no declared parameter-value contract beyond Wire's documented subset;
- no runtime identity or runtime-version introspection;
- no negotiated capability model.

The optional runtime identity is explicitly supplied by the host and is not
presented as Unified API introspection.

## Deferred

Class E work is outside this reset:

- the full handwritten client redesign; the existing facade temporarily maps
  `CompileOptions.Debuggable` to Compile versus CompileDebug, keeps legacy
  diagnostics empty, and leaves removed metadata/capability fields empty;
- advanced and negotiated capabilities;
- node or distributed bytecode transport.

## Summary

### Kept

The v1 package, logical Connect stream, explicit connection/resource release,
hierarchical ownership, finite non-blocking watches, caller-owned runtime and
listener, sanitized failures, reusable plans, and encoded output boundary.

### Changed

The handshake is flat, compilation distinguishes normal/debug operations,
optimization maps to Plan options, parameters are portable, watch messages
carry snapshots directly, command responses are empty, and debugger/source
data is complete.

### Removed

Native Ferret metadata, fake capabilities, diagnostics, resource-kind/error
duplication, implementation-specific parameter variants, redundant parent IDs,
transition wrappers, command snapshots, and the remote Runtime.Run shortcut.

### Unified API gaps

Diagnostics/error taxonomy, parameter-value declaration, runtime
identity/version introspection, and capability negotiation remain unavailable.

### Deferred

The full client rebuild, advanced capabilities, and distributed bytecode
transport remain separate tasks.
