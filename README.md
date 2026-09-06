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
     server.Server  <-------- ferret.wire.v1 ------ api.Runtime via client.New
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

`DefaultLimits` bounds client-controlled state to 64 logical connections; 128 plans, 128 normal sessions, and 128 executions per connection; 32 debug sessions per connection; 8 watchers per execution or debug session; 256 breakpoints per debug session; and 4 MiB for both inbound and outbound gRPC messages. Pending, active, and closing resources all count. Hosts may replace the complete positive limit set with `WithLimits`.

The one-shot Connect handshake publishes the connection ID, Wire protocol name and version, and optional host identity supplied through `WithRuntimeIdentity`. It does not publish fabricated capabilities, a Ferret version, or module-build metadata.

The Go client converts values supplied through `api.WithParam` and `api.WithParams` without reflection. It accepts `nil`, booleans, signed integer types, unsigned integers that fit in `int64`, finite `float32`/`float64`, strings, `[]byte`, `[]any`, and `map[string]any`. Duration, datetime, regexp, and other Go types are rejected locally.

See [Wire Protocol](docs/protocol.md) for every RPC/message/enum, lifecycle and watch semantics, compatibility classifications, Unified API gaps, and deferred work.

## Runtime host example

The host chooses and configures both the runtime implementation and endpoint. This function accepts caller-owned values and does not close either one:

```go
func serveRuntime(ctx context.Context, hostRuntime api.Runtime, listener net.Listener) error {
    wireServer, err := server.NewServer(hostRuntime, server.WithRuntimeIdentity(server.RuntimeIdentity{
        Name: "my-app", Version: "1.0.0", InstanceID: "worker-1",
    }))
    if err != nil {
        return err
    }

    return wireServer.Serve(ctx, listener)
}
```

`NewServer` accepts the canonical `api.Runtime` directly. `server.RuntimeIdentity`
is optional host-supplied handshake metadata.

For existing hosts, replace `server.Runtime` with `api.Runtime` and
`execution.Identity` with `server.RuntimeIdentity`. The old alias and identity
type were removed without compatibility shims; protocol and ownership behavior
are unchanged.

For an application-private Unix socket, the caller creates `net.Listen("unix", socket)`, applies appropriate directory and socket permissions, and closes both the listener and runtime after the Wire server has shut down.

## Remote runtime example

Configure the transport before constructing the remote runtime. For a private
Unix socket, the caller can use:

```go
conn, err := grpc.NewClient(
    "passthrough:///ferret-wire",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
        return new(net.Dialer).DialContext(ctx, "unix", "/var/run/my-app/ferret-wire.sock")
    }),
)
```

The caller checks the connection error and closes `conn` after its remote
runtimes. Credentials, TLS, dial options, and message limits belong to this
transport setup. `client.New` borrows the supplied connection and returns
`api.Runtime`; subsequent operations use the same interfaces as a local runtime:

```go
func runRemote(ctx context.Context, conn grpc.ClientConnInterface) (out api.Output, err error) {
    remote, err := client.New(ctx, conn)
    if err != nil {
        return api.Output{}, err
    }
    defer func() { err = errors.Join(err, remote.Close()) }()

    plan, err := remote.Compile(
        ctx,
        api.NewSource("example.fql", "RETURN @input"),
        api.WithOptimizationLevel(api.OptimizationBasic),
    )
    if err != nil {
        return api.Output{}, err
    }
    defer func() { err = errors.Join(err, plan.Close()) }()

    session, err := plan.NewSession(
        ctx,
        api.WithParam("input", "hello"),
        api.WithOutputContentType("application/json"),
    )
    if err != nil {
        return api.Output{}, err
    }
    defer func() { err = errors.Join(err, session.Close()) }()

    return session.Run(ctx)
}
```

For a one-shot invocation, `remote.Run(ctx, source, options...)` calls the hosted
`api.Runtime.Run` directly. Plans and durable sessions may be reused; normal
session runs are sequential. Output remains `api.Output`: content type and
encoded bytes.

For debugging, use `remote.CompileDebug`, `plan.NewDebugSession`, and the
canonical `api/debugger.Session` commands and events. Connection IDs, execution
handles, and Wire watch streams remain private.

The constructor context bounds the handshake. Cancelling it after construction
does not close the runtime. All resource `Close` methods use bounded detached
cleanup and leave `conn` open. Allocation replies that race cancellation are
reclaimed automatically. If a reply is lost, the adapter closes the nearest
owning session or plan and escalates to its logical runtime only when needed.
See [allocation and cancellation](docs/client.md#allocation-and-cancellation).

The public client exports only `New`, `Error`, `ErrClosed`, and
`ErrExecutionCancelled`. Existing users of `NewRuntime` should call `New`;
`client.Runtime`, `client.Session`, and `client.Output` declarations should use
the canonical `api` types. The previous lower-level handles, options, metadata,
and convenience operations have been removed without compatibility aliases.

Immediate failures expose a Wire `failure.Category` through `*client.Error`.
Terminal failures use `*failure.Failure`; both preserve canonical typed
diagnostics and sanitized messages. `errors.Is` distinguishes `ErrClosed`,
`ErrExecutionCancelled`, and caller context errors. Operation and cleanup
errors remain joined. Transport causes remain accessible through `Unwrap` and
`status.Code(err)` when transport-specific handling is needed. The API has no
general remote-error taxonomy to substitute for these Wire errors.

`server` hosts a borrowed `api.Runtime`, and `client` implements that interface
remotely. `pkg/execution`, `pkg/debugger`, and `pkg/failure` retain the domain
values shared by both sides. The module root has no Go compatibility package.

## Security and trust model

Wire supplies no default endpoint, authentication, authorization, TLS policy, TCP listener, named-pipe implementation, listener discovery, or externally reachable binding. Callers must choose and secure the listener, authenticate peers where required, enforce filesystem permissions for local sockets, and decide which runtime capabilities and host functions are safe for those peers. FQL source and parameters are trusted according to the host's policy; parameters may contain secrets and therefore require a confidential transport.

Compilation failures, execution failures, generic internal errors, and cleanup panics are sanitized and do not expose runtime error text, raw causes, panic values, filesystem paths, environment data, or host internals. Portable typed diagnostics may preserve the source content and semantic source name supplied to the runtime; source names are not assumed to be filesystem paths. Server limits reduce accidental and hostile resource exhaustion, but hosts must still decide which runtime capabilities are safe to expose.

Windows named pipes and remote TCP/TLS can be added later by supplying ordinary `net.Listener` and gRPC dialer implementations. Transport choice does not change the logical connection or protocol semantics.

## Non-goals and current limitations

Wire does not provide runtime introspection, Ferret module discovery, language intelligence, LSP, DAP translation, listener policy, downstream ferretd/CLI/Lab integration, TTLs, heartbeats, negotiated advanced capabilities, or node/distributed bytecode transport. Wire makes no changes to Ferret core or other MontFerret repositories.

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

The [Universal API integration suite](test/integration/README.md) exercises the
public client/server boundary over real gRPC using an in-memory `bufconn`
transport and hosted API spies. Run it independently with
`go test ./test/integration/...` or `go test -race ./test/integration/...`.
Package-local tests retain component, conversion, and low-level protocol coverage.

CI invokes these Make targets on Linux, macOS, and Windows; Linux additionally
runs the race detector, Buf lint, checked generation, and pull-request breaking
checks against the fetched base branch. The integration suite is included in
the existing `./...` targets without build tags or extra services.
