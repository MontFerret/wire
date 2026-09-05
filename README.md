# Ferret Wire

Ferret Wire is a versioned gRPC boundary for hosting an implementation of the [Unified Ferret API](https://github.com/MontFerret/api) in another process. It lets a host expose compilation, execution, and source-level debugging without moving runtime construction, configuration, policy, or listener security into this library.

This module targets Go 1.25 and Unified API `v1.0.0-alpha.11`. The v1 protobuf package is `ferret.wire.v1`; its sources live in `proto/ferret/wire/v1`, and the checked-in Go bindings live in `gen/ferret/wire/v1`.

## Ownership and architecture

```text
host application                         client application
  owns configured api.Runtime              owns grpc.ClientConnInterface
  owns and secures net.Listener             owns transport lifetime
             |                                         |
             v                                         v
     server.Server  <-------- ferret.wire.v1 ------ client.Runtime / Client
        borrows runtime                           owns Connect stream
             |
       logical Connection
        ├── direct Runtime executions
        └── Plans
             ├── direct Executions
             ├── durable Sessions ── Executions
             └── Debug sessions
```

`NewServer` only constructs state. It does not listen, dial, inspect the environment, or close the supplied runtime. `Serve` is the only operation that accepts a listener, and the caller retains responsibility for the endpoint. `Shutdown` releases Wire-owned resources while leaving the runtime open.

Every `Connect` server stream creates one logical ownership scope. It is deliberately independent of the physical HTTP/2 connection: several logical connections can share one `grpc.ClientConn`, but their IDs and resources remain isolated. Cancelling the Connect stream or calling `CloseConnection` first cancels and waits for pending creation, then settles executions, normal sessions, debug sessions, and plans in descendants-first order. Concurrent callers that observe the same in-flight release wait for its retained result. Once cleanup completes, the ID is stale and returns the corresponding structured not-found error. Cancelling one waiter does not abandon committed cleanup.

Unary execution and debug resume calls publish work before returning. Once published, work runs under the logical Connect lifecycle. Compilation, normal/debug-session construction, and frame evaluation combine unary cancellation with logical lifecycle cancellation. Cancelling a watch only detaches that watcher. A watcher first receives the current snapshot, then future events through a buffer of eight; a lagging watcher is detached with `ResourceExhausted` and cannot block the underlying work. Its watcher slot remains occupied until the stream handler exits.

## Protocol contracts

- `Compile` and `CompileDebug` create the same reusable Plan resource, containing only its opaque ID and declared parameters. Optional optimization levels map to Unified API plan options; an unspecified level preserves the runtime default.
- `CreateSession` constructs one durable hosted `api.Session`. `RunSession` reuses it for sequential runs and represents every run as a distinct asynchronous Execution; overlap is rejected until the prior Execution is released.
- `RuntimeService.Run` invokes the hosted `api.Runtime.Run` directly and represents that one-shot operation as a connection-owned asynchronous Execution. It does not compile a temporary Plan.
- Parameter values use an explicit protobuf oneof for null, boolean, exact signed 64-bit integer, finite double, string, bytes, array, or string-keyed object. Missing variants, NaN, infinities, custom values, and nesting beyond 64 levels are rejected. In protobuf JSON, int64 values are decimal strings while finite doubles remain JSON numbers.
- Execution and debug completion carry one shared Unified API output contract unchanged: `content_type` plus encoded `content` bytes. Wire never decodes or reinterprets them.
- Execution and debug watches carry ordered snapshots. State is the execution lifecycle discriminator; debug events also carry a kind because start and continue both publish a running state. A new debug session immediately publishes a created snapshot. Created, running, stopped, and terminal snapshots are replayable; watcher cancellation is independent of resource cancellation, and slow watchers are detached without blocking runtime work.
- Debug transport uses the canonical `StepOver`, `StepIn`, and `StepOut` commands and preserves semantic source names, ranges and spans, event depth, requested and resolved breakpoints, binding mode, point and function IDs, frame function IDs, variables, value references, stop reason, and hit breakpoint IDs. Positive value references are scoped to the current stopped state. Frame order is the zero-based index accepted by `FrameLocals` and `EvaluateFrame`.
- Invalid requests, cancellation, and resource exhaustion use normal gRPC status codes. `ErrorDetail` carries meaningful Wire lifecycle/runtime categories. Typed Unified API diagnostics are preserved separately from sanitized summaries on immediate errors and asynchronous failures; Wire never parses arbitrary runtime error strings.

`DefaultServerLimits` bounds client-controlled state to 64 logical connections; 128 plans, 128 normal sessions, and 128 executions per connection; 32 debug sessions per connection; 8 watchers per execution or debug session; 256 breakpoints per debug session; and 4 MiB for both inbound and outbound gRPC messages. Pending, active, and closing resources all count. Hosts may replace the complete positive limit set with `WithServerLimits`.

The one-shot Connect handshake publishes the connection ID, Wire protocol name and version, and optional host identity supplied through `WithRuntimeIdentity`. It does not publish fabricated capabilities, a Ferret version, or module-build metadata.

The Go client converts parameters without reflection. `client.Parameters` accepts `nil`, booleans, signed integer types, unsigned integers that fit in `int64`, finite `float32`/`float64`, strings, `[]byte`, `[]any`, and `map[string]any`. Duration, datetime, regexp, and other Go types are rejected locally.

See [Wire Protocol](docs/protocol.md) for every RPC/message/enum, lifecycle and watch semantics, compatibility classifications, Unified API gaps, and deferred work.

## Runtime host example

The host chooses and configures both the runtime implementation and endpoint. This function accepts caller-owned values and does not close either one:

```go
func serveRuntime(ctx context.Context, hostRuntime api.Runtime, listener net.Listener) error {
    wireServer, err := server.NewServer(hostRuntime, server.WithRuntimeIdentity(execution.Identity{
        Name: "my-app", Version: "1.0.0", InstanceID: "worker-1",
    }))
    if err != nil {
        return err
    }

    return wireServer.Serve(ctx, listener)
}
```

For an application-private Unix socket, the caller creates `net.Listen("unix", socket)`, applies appropriate directory and socket permissions, and closes both the listener and runtime after the Wire server has shut down.

The caller owns the gRPC transport. Closing either Wire client facade closes
only its logical connection and the remote resources created through it.

The Universal API facade is the primary path for callers that do not need
Wire-specific asynchronous handles:

```go
remoteRuntime, err := client.NewRuntime(ctx, conn)
if err != nil {
    log.Fatal(err)
}
defer func() {
    if err := remoteRuntime.Close(); err != nil {
        log.Printf("close remote runtime: %v", err)
    }
}()

output, err := remoteRuntime.Run(
    ctx,
    api.NewSource("example.fql", "RETURN @input"),
    api.WithParam("input", "hello"),
    api.WithOutputContentType("application/json"),
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%s: %s\n", output.ContentType, output.Content)
```

`client.Runtime` implements `api.Runtime`. Its returned Plans, normal Sessions,
and debugger Sessions implement the corresponding Universal API interfaces.
Plans and durable Sessions may be reused; normal Session runs are sequential.
All adapter `Close` methods use bounded detached cleanup and never close `conn`.

Allocation replies that race cancellation are reclaimed automatically. If a
reply is lost, the adapter closes the nearest owning Session or Plan and
escalates to its logical Runtime only when needed. The caller's gRPC transport
remains open. See [allocation and cancellation](docs/client.md#allocation-and-cancellation)
for the bounded cleanup contract.

The lower-level `client.Client` remains available. Its common one-shot path creates and releases
its plan and execution automatically:

```go
output, err := wireClient.Run(
    ctx,
    api.NewSource("example.fql", "RETURN @input"),
    client.Parameters{"input": "hello"},
    client.RunOptions{},
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("%s: %s\n", output.ContentType, output.Content)
```

Use explicit handles when plans must be reused or execution needs watching,
cancellation, or separately reported cleanup:

```go
const socket = "/var/run/my-app/ferret-wire.sock"
conn, err := grpc.NewClient(
    "passthrough:///ferret-wire",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
        return new(net.Dialer).DialContext(ctx, "unix", socket)
    }),
)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

wireClient, err := client.New(ctx, conn)
if err != nil {
    log.Fatal(err)
}
defer func() {
    if err := wireClient.Close(context.Background()); err != nil {
        log.Printf("close Wire client: %v", err)
    }
}()

plan, err := wireClient.Compile(
    ctx,
    api.NewSource("example.fql", "RETURN {input: @input}"),
    client.CompileOptions{
        PlanOptions: []api.PlanOption{api.WithOptimizationLevel(api.OptimizationBasic)},
    },
)
if err != nil {
    log.Fatal(err)
}
defer func() {
    if err := plan.Close(context.Background()); err != nil {
        log.Printf("close plan: %v", err)
    }
}()

execution, err := plan.Execute(ctx, map[string]any{"input": "hello"}, client.ExecuteOptions{})
if err != nil {
    log.Fatal(err)
}
defer func() {
    if err := execution.Close(context.Background()); err != nil {
        log.Printf("close execution: %v", err)
    }
}()

events, err := execution.Watch(ctx)
if err != nil {
    log.Fatal(err)
}
for {
    event, err := events.Recv()
    if err != nil {
        log.Fatal(err)
    }
    if event.Snapshot.State == execution.StateCompleted {
        fmt.Printf("%s: %s\n", event.Snapshot.Output.ContentType, event.Snapshot.Output.Content)
        break
    }
    if event.Snapshot.State.Terminal() {
        log.Fatalf("execution ended in state %v: %v", event.Snapshot.State, event.Snapshot.Failure)
    }
}
```

The public Go API is split by ownership: `server` hosts a borrowed `api.Runtime`,
`client` owns remote handles, and `pkg/execution`, `pkg/debugger`, and
`pkg/failure` contain the semantic values shared by both sides. The module root
intentionally has no Go compatibility package.

Wire failures expose `failure.Category` through `*client.Error`. When the
runtime returns typed `diagnostics.Diagnostics`, `*client.Error` and
asynchronous `*failure.Failure` preserve that canonical collection, including
source content and ordered annotations. Bare cancellation, deadline,
invalid-request, unavailable, and resource-exhaustion statuses remain
category-free and are classified with `status.Code(err)`. Remote connection and
resource IDs are not part of the high-level client error model.

## Security and trust model

Wire supplies no default endpoint, authentication, authorization, TLS policy, TCP listener, named-pipe implementation, listener discovery, or externally reachable binding. Callers must choose and secure the listener, authenticate peers where required, enforce filesystem permissions for local sockets, and decide which runtime capabilities and host functions are safe for those peers. FQL source and parameters are trusted according to the host's policy; parameters may contain secrets and therefore require a confidential transport.

Compilation failures, execution failures, generic internal errors, and cleanup panics are sanitized and do not expose runtime error text, raw causes, panic values, filesystem paths, environment data, or host internals. Portable typed diagnostics may preserve the source content and semantic source name supplied to the runtime; source names are not assumed to be filesystem paths. Server limits reduce accidental and hostile resource exhaustion, but hosts must still decide which runtime capabilities are safe to expose.

Windows named pipes and remote TCP/TLS can be added later by supplying ordinary `net.Listener` and gRPC dialer implementations. Transport choice does not change the logical connection or protocol semantics.

## Non-goals and current limitations

Wire does not provide runtime introspection, Ferret module discovery, language intelligence, LSP, DAP translation, listener policy, downstream ferretd/CLI/Lab integration, TTLs, heartbeats, negotiated advanced capabilities, or node/distributed bytecode transport. Further lower-level client redesign is separate from the Universal API facade. Wire makes no changes to Ferret core or other MontFerret repositories.

Wire forwards cancellation to Unified API compile and session operations. Whether
an implementation can promptly interrupt its internal work remains a runtime
concern; Wire does not attempt implementation-specific interruption. It also
does not synthesize intermediate output from logs.

## Development

```sh
make fmt              # format handwritten Go
make check-fmt        # verify formatting without changing files
make generate         # regenerate checked-in Go/gRPC bindings
make check-generate   # fail when generation changes the checkout
make proto-lint       # Buf STANDARD lint
make proto-breaking BUF_BREAKING_AGAINST=.git#branch=main
make check-tidy       # verify go.mod/go.sum without changing files
make vet
make test
make test-race
make build
```

Tests use in-memory `bufconn` transports and direct lifecycle coverage. CI invokes these Make targets on Linux, macOS, and Windows; Linux additionally runs the race detector, Buf lint, checked generation, and pull-request breaking checks against the fetched base branch.
