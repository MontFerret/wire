# Ferret Wire

Ferret Wire is a versioned gRPC boundary for embedding a configured [Ferret](https://github.com/MontFerret/ferret) engine in another process. It lets a host expose compilation, execution, and source-level debugging without moving engine construction, module registration, network policy, or listener security into this library.

This module targets Go 1.25 and Ferret `v2.0.0-alpha.50`. The v1 protobuf package is `ferret.wire.v1`; its sources live in `proto/ferret/wire/v1`, and the checked-in Go bindings live in `gen/ferret/wire/v1`.

## Ownership and architecture

```text
host application                         client application
  owns configured *ferret.Engine           owns grpc.ClientConnInterface
  owns and secures net.Listener             owns transport lifetime
             |                                         |
             v                                         v
       wire.Server  <-------- ferret.wire.v1 ------ client.Client
        borrows engine                            owns Connect stream
             |
       logical Connection
        ├── Plans
        │    ├── Executions
        │    └── Debug sessions
        └── fixed-buffer watchers
```

`NewServer` only constructs state. It does not listen, dial, inspect the environment, or close the supplied engine. `Serve` is the only operation that accepts a listener, and the caller retains responsibility for the endpoint.

Every `Connect` server stream creates one logical ownership scope. It is deliberately independent of the physical HTTP/2 connection: several logical connections can share one `grpc.ClientConn`, but their IDs and resources remain isolated. Cancelling the Connect stream or calling `CloseConnection` cancels and settles debug sessions and executions before releasing plans. Release calls are idempotent; concurrent callers wait for the same retained cleanup result. Cancelling one waiter does not abandon committed cleanup.

Unary execution and debug resume calls publish work before returning. Once published, work runs under the Connect stream context rather than the unary request context. Cancelling a watch only detaches that watcher. A watcher first receives the current snapshot, then future events through a buffer of eight; a lagging watcher is detached with `ResourceExhausted` and cannot block the underlying work.

## Protocol contracts

- Compilation returns opaque UUIDs, declared parameters, debug capability, and Ferret diagnostics. Diagnostic locations are labeled, half-open UTF-8 byte spans, not LSP ranges.
- Parameter values use an explicit protobuf oneof: none, boolean, signed 64-bit integer, double, string, binary, duration nanoseconds, RFC3339Nano datetime, regexp, array, or string-keyed object. Missing variants, malformed values, and nesting beyond 64 levels are rejected.
- Execution and debug completion carry Ferret's canonical output contract unchanged: content type plus encoded bytes. The current Ferret APIs do not expose intermediate runtime or debugger output, so Wire emits terminal output only even though v1 reserves intermediate output events.
- Debug sessions are created before execution so breakpoints can be replaced deterministically. Source lines and columns are 1-based, frame indices are zero-based, and expandable value references become stale after every resume.
- Unary failures use normal gRPC codes and an attached `ferret.wire.v1.ErrorDetail`. Compilation errors include structured diagnostics. Execution and debug failures are terminal events so watchers observe one ordered outcome.
- The server sets an explicit 4 MiB inbound gRPC message limit.

The exact Ferret module version is read from Go build metadata. Development and replaced builds, whose dependency version is unavailable, fall back to `compiler.Version`.

The Go client converts parameters without reflection. `client.Parameters` accepts `nil`, booleans, signed integer types, unsigned integers that fit in `int64`, `float32`/`float64`, strings, `[]byte`, `time.Duration`, `time.Time`, `regexp.Regexp` or `*regexp.Regexp`, `[]any`, and `map[string]any`. Other Go types are rejected locally.

## Unix-socket example

The host chooses the endpoint and configures the engine. This example uses an application-private Unix socket; production code should also set appropriate directory and socket permissions.

```go
engine, err := ferret.New(
    ferret.WithFunctionsRegistrar(func(ns runtime.Namespace) {
        ns.Function().A0().Add("HOST_NAME", func(context.Context) (runtime.Value, error) {
            return runtime.NewString("example-host"), nil
        })
    }),
)
if err != nil {
    log.Fatal(err)
}
defer engine.Close() // the application, not Wire, owns the engine

const socket = "/var/run/my-app/ferret-wire.sock"
listener, err := net.Listen("unix", socket)
if err != nil {
    log.Fatal(err)
}
defer listener.Close()

server, err := wire.NewServer(engine, wire.WithRuntimeIdentity(wire.RuntimeIdentity{
    Name: "my-app", Version: "1.0.0", InstanceID: "worker-1",
}))
if err != nil {
    log.Fatal(err)
}
if err := server.Serve(ctx, listener); err != nil {
    log.Fatal(err)
}
```

The client owns its gRPC transport. Closing the Wire client closes only its logical connection.

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
defer wireClient.Close(context.Background())

plan, err := wireClient.Compile(ctx, client.Source{
    Identity: "example.fql",
    Content:  "RETURN {host: HOST_NAME(), input: @input}",
}, client.CompileOptions{})
if err != nil {
    log.Fatal(err)
}
execution, err := wireClient.Execute(ctx, plan.ID, map[string]any{"input": "hello"}, client.ExecuteOptions{})
if err != nil {
    log.Fatal(err)
}
events, err := wireClient.WatchExecution(ctx, execution.ID)
if err != nil {
    log.Fatal(err)
}
for {
    event, err := events.Recv()
    if err != nil {
        log.Fatal(err)
    }
    if event.Kind == client.ExecutionEventCompleted {
        fmt.Printf("%s: %s\n", event.Execution.Output.ContentType, event.Execution.Output.Data)
        break
    }
}
```

## Security and trust model

Wire supplies no default endpoint, authentication, authorization, TLS policy, TCP listener, named-pipe implementation, listener discovery, or externally reachable binding. Callers must choose and secure the listener, authenticate peers where required, enforce filesystem permissions for local sockets, and decide which Ferret capabilities and host functions are safe for those peers. FQL source and parameters are trusted according to the host's policy; parameters may contain secrets and therefore require a confidential transport.

Generic internal errors are sanitized and do not expose raw causes, panic values, filesystem paths, environment data, or host internals. Expected Ferret compilation/runtime diagnostics and the source identity supplied by the caller remain observable protocol data.

Windows named pipes and remote TCP/TLS can be added later by supplying ordinary `net.Listener` and gRPC dialer implementations. Transport choice does not change the logical connection or protocol semantics.

## Non-goals and current limitations

Wire does not provide runtime introspection, Ferret module discovery, language intelligence, LSP, DAP translation, listener policy, downstream ferretd/CLI/Lab integration, TTLs, or heartbeats. It makes no changes to Ferret core or other MontFerret repositories.

Ferret accepts a context at the engine compilation boundary, but compiler CPU work is not internally cancellable. Wire does not attempt to interrupt or work around that implementation detail. Likewise, it does not synthesize intermediate output from logs.

## Development

```sh
make proto-lint       # Buf STANDARD lint
make generate         # regenerate checked-in Go/gRPC bindings
make check-generate   # fail when generation changes the checkout
make test
make vet
```

Tests use in-memory `bufconn` transports and direct lifecycle coverage. CI runs test, vet, and build on Linux, macOS, and Windows; Linux additionally runs the race detector and validates Buf lint and checked generation.
