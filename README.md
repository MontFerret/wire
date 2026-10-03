# Ferret Wire

Ferret Wire is a versioned gRPC boundary for hosting an implementation of the [Unified Ferret API](https://github.com/MontFerret/api) in another process. It lets a host expose compilation, execution, and source-level debugging while the host retains runtime construction, configuration, and security policy.

This module targets Go 1.25 and Unified API `v1.0.0-alpha.20`. The v1 protobuf package is `ferret.wire.v1`; its sources live in `proto/ferret/wire/v1`, and the checked-in Go bindings live in `gen/ferret/wire/v1`.

## Ownership and architecture

```text
host application                         client application
  owns configured api.Runtime              chooses New or From
  chooses endpoint and security policy     closes runtimes and descendants
             |                                         |
             v                                         v
     server.Server  <-------- ferret.wire.v1 ------ api.Runtime via client.New/From
        borrows runtime                           owns Connect stream
             |
       logical Connection
        ├── direct Runtime executions
        └── Plans
             ├── direct Executions
             ├── durable Sessions ── Executions
             └── Debug sessions
```

`server.New` only constructs state. It does not listen, dial, inspect the environment, or close the supplied runtime. `Run` explicitly creates a TCP listener at the supplied address and manages serving and shutdown. `Serve` accepts a caller-created listener, including Unix sockets and custom transports; gRPC closes that listener when serving returns. Both use the same constructor-configured security policy. `Shutdown` releases Wire-owned resources while leaving the runtime open.

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

The one-shot Connect handshake publishes four distinct values: `connection_id`
identifies the Wire lifecycle scope; `protocol` identifies Wire; `runtime_version`
is the exact opaque value from the hosted `api.Runtime.Version(ctx)`; and optional
`runtime_identity` is host-supplied application or instance identity configured
through `WithRuntimeIdentity`. `runtime_version` and `runtime_identity.version`
have different owners and semantics, even when their strings happen to match.
Runtime version bytes are preserved exactly, including invalid UTF-8. An empty
runtime version is valid when present; alpha.20 clients reject handshakes
without the field. Wire exposes no capability negotiation, Ferret-specific build
metadata, Git metadata, or module inventories.

The Go client converts values supplied through `api.WithParam` and `api.WithParams` without reflection. It accepts `nil`, booleans, signed integer types, unsigned integers that fit in `int64`, finite `float32`/`float64`, strings, `[]byte`, `[]any`, and `map[string]any`. Duration, datetime, regexp, and other Go types are rejected locally.

See [Wire Protocol](docs/protocol.md) for every RPC/message/enum, lifecycle and watch semantics, compatibility classifications, Unified API gaps, and deferred work.

## Runtime host example

The host chooses and configures the runtime implementation and endpoint. The minimal managed TCP path is:

```go
func serveRuntime(ctx context.Context, hostRuntime api.Runtime) error {
    wireServer, err := server.New(hostRuntime)
    if err != nil {
        return err
    }

    return wireServer.Run(ctx, "127.0.0.1:50051")
}
```

This example uses **unencrypted, unauthenticated transport**, even on loopback.
The matching client explicitly selects plaintext and owns its channel:

```go
func runLoopback(ctx context.Context) (out *api.Output, err error) {
    remote, err := client.New(ctx, "127.0.0.1:50051", client.WithInsecure())
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, remote.Close()) }()

    return remote.Run(ctx, api.NewAnonymousSource("RETURN 1"))
}
```

`client.New(ctx, target, options...)` completes the Wire handshake before
returning. Its default is TLS with system trust and server identity verification;
there is no automatic plaintext fallback, including on loopback or Unix sockets.
`WithInsecure` disables encryption and peer authentication. Custom credentials
and per-RPC authentication use `WithTransportCredentials` and
`WithPerRPCCredentials`; see [security examples](docs/security.md).

The constructor context controls startup only. Cancellation after successful
construction does not close the runtime. Failed construction returns a nil
runtime and rolls back with bounded detached cleanup, which may take additional
time after startup cancellation. Closing a runtime gates new root calls, while
admitted work and retained plans, sessions, and debuggers keep their lifetimes.
The owned channel closes after final logical release, even if cleanup fails.
Callers must still close all returned descendants.

| Constructor | Logical Wire connection | gRPC channel |
| --- | --- | --- |
| `New(ctx, target, options...)` | Managed by Wire | Created and eventually closed by Wire |
| `From(ctx, connection)` | Managed by Wire | Always caller-owned |

`Run` returns after managed shutdown settles. Serving-context cancellation or
explicit shutdown returns `nil` when cleanup succeeds; serving and cleanup errors
are returned together. Its default shutdown budget is 30 seconds, starting when
shutdown begins. Override it with
`wireServer.Run(ctx, "127.0.0.1:50051", server.WithShutdownTimeout(10*time.Second))`.
An earlier explicit `Shutdown` deadline shortens that budget; a later deadline or
`Shutdown(context.Background())` cannot extend it. The canceled serving context
is not the cleanup context.

A timeout matches `context.DeadlineExceeded`: transport is forced to stop, but
uncooperative hosted cleanup may still be running. Later `Shutdown` calls wait
for its retained result without repeating cleanup. The host must allow that
cleanup to settle before closing its borrowed runtime.

For caller-created listeners, retain the explicit serving/waiting path:

```go
func serveListener(ctx context.Context, hostRuntime api.Runtime, listener net.Listener) error {
    wireServer, err := server.New(hostRuntime)
    if err != nil {
        return err
    }

    serveErr := wireServer.Serve(ctx, listener)
    return errors.Join(serveErr, wireServer.Shutdown(context.Background()))
}
```

`Serve` has no managed default timeout; the host chooses its shutdown context.
Concurrent/repeated starts are rejected without disturbing the accepted start.
A failed bind permits retry until serving or shutdown commits.

`New` and `Run` invoke every non-nil option in registration order and join all
validation failures before constructing the server or reserving startup. Nil
options remain errors. Invalid options never apply their setters, and a later
valid override cannot erase an earlier failure. Named validation errors support
`errors.As` inspection; see [option validation](docs/security.md#option-validation).
Collection errors use relative indices or field-key labels; ordering among
invalid limit fields is unspecified.

`New` accepts the canonical `api.Runtime` directly. `server.RuntimeIdentity`
is optional host-supplied handshake metadata.

For existing hosts, replace `server.Runtime` with `api.Runtime` and
`execution.Identity` with `server.RuntimeIdentity`. The old alias and identity
type were removed without compatibility shims; protocol and ownership behavior
are unchanged.

For an application-private Unix socket, the caller creates `net.Listen("unix", socket)`, applies appropriate directory and socket permissions, and closes its runtime after Wire cleanup settles. gRPC closes the accepted listener; a rejected `Serve` leaves it untouched.

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
transport setup. `client.From` borrows the supplied connection and returns
`api.Runtime`; subsequent operations use the same interfaces as a local runtime:

```go
func runRemote(ctx context.Context, conn grpc.ClientConnInterface) (out *api.Output, err error) {
    remote, err := client.From(ctx, conn)
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, remote.Close()) }()

    plan, err := remote.Compile(
        ctx,
        api.NewSource("example.fql", "RETURN @input"),
        api.WithOptimizationLevel(api.OptimizationBasic),
    )
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, plan.Close()) }()

    session, err := plan.NewSession(
        ctx,
        api.WithParam("input", "hello"),
        api.WithOutputContentType("application/json"),
    )
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, session.Close()) }()

    return session.Run(ctx)
}
```

For a one-shot invocation, `remote.Run(ctx, source, options...)` calls the hosted
`api.Runtime.Run` directly. Plans and durable sessions may be reused; normal
session runs are sequential. Output is `*api.Output`: content type and
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

Ordinary `Runtime.Close` rejects new root calls and defers connection teardown
until admitted work and caller-owned descendants finish. `Plan.Close` closes
only the hosted plan after admitted constructors settle; existing children
survive. Close every returned child. Transport release and lost-allocation
recovery still cascade. Output presence and available output accompanying an
error are preserved. Filesystem roots and output content types preserve explicit
empty settings; the hosted runtime owns their validation. `Runtime.Version(ctx)`
reads the Connect snapshot and `Plan.Params(ctx)` returns a defensive copy of the
parameter snapshot captured during compile. Both validate non-nil caller contexts,
preserve cancellation/deadline errors, and remain readable after cleanup without
another RPC.

Debugger methods take caller contexts. `RunCommand` streams preserve Start's
execution lifetime and each resume's request context; cancellation does not
release the debugger. Atomic breakpoint replacement works while running, and
breakpoint enumeration remains available after explicit Close. See the
[alpha.20 interface coverage](test/integration/README.md#interface-coverage).

The public client exports `New`, `From`, `Option`, the three transport option
factories, `Error`, `ErrClosed`, and `ErrExecutionCancelled`. This is an intentional
Go constructor API break: existing `New(ctx, conn)` calls become `From(ctx, conn)`
with unchanged caller ownership. Existing users of `NewRuntime` also use `From`;
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

Hosts configure TLS or mTLS with `server.WithTransportCredentials`, and
per-call authentication/authorization with `server.WithUnaryInterceptors` and
`server.WithStreamInterceptors`. These options apply to every corresponding RPC
across all Wire services, through both `Run` and `Serve`. Wire recovery wraps host
middleware, and configured message/resource limits remain enforced. Without
transport credentials, Wire provides no encryption or authentication. Configured
credentials have no fallback to plaintext.

See [Security configuration](docs/security.md) for connected TLS, mTLS, and
TLS-plus-token host/client examples. Tokens belong in connection-level client
`PerRPCCredentials` requiring transport security, so subsequent unary and
streaming operations carry them too.

Authentication is not tenant isolation: Wire does not bind resource ownership to
an authenticated principal. Stream authentication occurs at establishment;
revocation and expiry enforcement for already-open streams remain host policy.
Hosts choose listener exposure, filesystem permissions for local sockets, and
runtime capabilities safe to expose. Wire supplies no default endpoint,
certificate management, built-in token validation, or implicit public binding.
Source and parameters may contain secrets and require a confidential transport.

Compilation failures, execution failures, generic internal errors, and cleanup panics are sanitized and do not expose runtime error text, raw causes, panic values, filesystem paths, environment data, or host internals. Portable typed diagnostics may preserve the source content and semantic source name supplied to the runtime; source names are not assumed to be filesystem paths. Server limits reduce accidental and hostile resource exhaustion, but hosts must still decide which runtime capabilities are safe to expose.

Custom transports such as Windows named pipes use caller-supplied `net.Listener` and gRPC dialer implementations. Managed `Run` serves TCP; TLS is constructor configuration for either path. Transport choice does not change the logical connection or protocol semantics.

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
make lint             # verify configuration and lint the complete Go baseline
make generate         # regenerate checked-in Go/gRPC bindings
make check-generate   # fail when generation changes the checkout
make proto-lint       # Buf STANDARD lint
make proto-breaking BUF_BREAKING_AGAINST=.git#branch=main
make check-tidy       # verify go.mod/go.sum without changing files
make check-ferret-tidy # verify the native compatibility module's dependencies
make vet
make test             # root module, including hosted-spy integration contracts
make test-ferret      # nested module, including native Ferret round trips
make test-race
make test-ferret-race
make build
```

The Makefile pins golangci-lint and installs its official, checksum-verified
release under ignored `bin/tools/golangci-lint/<version>/`. `make fmt`,
`make check-fmt`, and `make lint` install it automatically when the selected
version's executable is absent and reuse it afterward. Explicit installation
with `make install-lint` is optional. First use requires download access, `curl`,
and a POSIX shell (such as Git Bash on Windows). Tool selection uses the host
platform even when Go cross-compilation variables are set. `make build` remains
a compilation-only target. Formatting, formatting checks, lint, and vet cover
both the root module and the nested native-Ferret test module using the same
tool configuration.

[The lint configuration](.golangci.yml) enables correctness, error handling,
resource cleanup, spelling, API documentation and naming, grouped type
declarations, and control-flow spacing checks. Its explicit linter list is
`errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`, `bodyclose`,
`errorlint`, `copyloopvar`, `nolintlint`, `revive`, `grouper`, `misspell`,
and `wsl_v5`. Formatting uses `gofmt` and `goimports`, with
`github.com/MontFerret` imports grouped together. Tests are covered; generated
code and vendor directories are excluded from lint and formatting. Linting is
read-only and analyzes the full baseline.

Fix findings at their source. When a check conflicts with an intentional
contract, use a narrow directive such as
`//nolint:errorlint // Verify the original error is returned unchanged.`
Suppressions must name the linter, explain the reason, and suppress a real
finding. Preserve exact-error and panic-identity assertions. A configuration
exception covers only capitalization warnings for error literals beginning
with the proper name "Wire"; other Staticcheck checks remain enabled.
Architectural and ownership rules still require review.

There are two complementary integration layers, both using real gRPC over
`bufconn` without external services:

| Suite | Hosted implementation | Responsibility |
| --- | --- | --- |
| [Wire contracts](test/integration/README.md) | UAPI spies | Exhaustive Wire semantics, faults, cancellation, ownership, and limits |
| [Native Ferret compatibility](test/ferret/README.md) | Native engine through `ferret/uapi` | Focused execution, session, lifecycle, diagnostic, and debugger round trips |

Run the contract suite independently with `go test ./test/integration/...` or
`go test -race ./test/integration/...`. The native suite is a separate module,
so root `go test ./...` intentionally excludes it; use `make test-ferret` and
`make test-ferret-race`. It pins released Ferret and UAPI versions and replaces
only Wire with the local checkout. Native Ferret remains absent from Wire's
root module and production imports; no repository-wide `go.work` is needed.
Package-local tests retain component, conversion, and low-level protocol coverage.

CI invokes these Make targets on Linux, macOS, and Windows; Linux additionally
runs Go lint, the race detector, Buf lint, checked generation, and pull-request
breaking checks against the fetched base branch. Both integration layers run on
the OS matrix and under the Linux race detector. CI explicitly invokes the
nested-module targets and caches dependencies using both modules' checksum files.
