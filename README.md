# Ferret Wire

Ferret Wire is a versioned gRPC boundary for exposing a host application's
configured [Ferret](https://github.com/MontFerret/ferret) engine to external Go
tooling. It supports compilation, execution, and source-level debugging without
making Wire responsible for engine configuration or endpoint security.

The public client model is intentionally small:

```text
Client
└── Plan
    ├── Execution
    └── DebugSession
```

## Installation

Wire requires Go 1.25. Add it to a Go module with:

```sh
go get github.com/MontFerret/wire@latest
```

## Hosting Wire

The host configures and owns the Ferret engine and listener. Wire serves that
engine over the supplied endpoint:

```go
func serve(ctx context.Context) error {
	// Configure Ferret functions, modules, and policies for the host here.
	engine, err := ferret.New()
	if err != nil {
		return err
	}
	defer engine.Close()

	listener, err := net.Listen("unix", "/var/run/my-app/ferret-wire.sock")
	if err != nil {
		return err
	}
	defer listener.Close()

	server, err := wire.NewServer(engine)
	if err != nil {
		return err
	}

	return server.Serve(ctx, listener)
}
```

Secure the listener for the capabilities exposed by the configured engine.

## One-shot execution

`Client.Run` is the simplest way to compile and execute one FQL program. It
releases the temporary plan and execution; the caller closes the logical Wire
client and its gRPC connection.

This complete example connects to an application-private Unix socket:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/MontFerret/wire/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := runOnce(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func runOnce(ctx context.Context) (err error) {
	conn, err := grpc.NewClient(
		"unix:///var/run/my-app/ferret-wire.sock",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, conn.Close())
	}()

	wireClient, err := client.New(ctx, conn)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, wireClient.Close(context.Background()))
	}()

	output, err := wireClient.Run(
		ctx,
		client.Source{Identity: "example.fql", Content: "RETURN @input"},
		client.Parameters{"input": "hello"},
		client.RunOptions{
			Execute: client.ExecuteOptions{OutputContentType: "application/json"},
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("content type: %s\n", output.ContentType)
	_, err = os.Stdout.Write(output.Content)

	return err
}
```

The example uses transport credentials without TLS because access is restricted
by the local socket. Use appropriate TLS and authentication for remote
endpoints.

## Choosing an execution API

- Use `Client.Run` for one-shot execution.
- Use explicit `Plan` and `Execution` handles for plan reuse, cancellation,
  event watching, debugging, or direct lifecycle control.

The explicit path compiles, executes, waits, and closes child resources before
their parent:

```go
func runExplicit(ctx context.Context, wireClient *client.Client) (output client.Output, err error) {
	plan, err := wireClient.Compile(
		ctx,
		client.Source{Identity: "example.fql", Content: "RETURN @input"},
		client.CompileOptions{},
	)
	if err != nil {
		return client.Output{}, err
	}
	defer func() {
		err = errors.Join(err, plan.Close(context.Background()))
	}()

	execution, err := plan.Execute(
		ctx,
		client.Parameters{"input": "hello"},
		client.ExecuteOptions{OutputContentType: "application/json"},
	)
	if err != nil {
		return client.Output{}, err
	}
	defer func() {
		err = errors.Join(err, execution.Close(context.Background()))
	}()

	return execution.Wait(ctx)
}
```

## Debugging

Debug sessions are created from plans compiled with debugging enabled. This
example starts a session and receives its first published event:

```go
func debugOnce(ctx context.Context, wireClient *client.Client) (err error) {
	plan, err := wireClient.Compile(
		ctx,
		client.Source{Identity: "example.fql", Content: "RETURN @input"},
		client.CompileOptions{Debuggable: true},
	)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, plan.Close(context.Background()))
	}()

	debugSession, err := plan.NewDebugSession(
		ctx,
		client.Parameters{"input": "hello"},
		client.DebugSessionOptions{OutputContentType: "application/json"},
	)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, debugSession.Close(context.Background()))
	}()

	events, err := debugSession.Watch(ctx)
	if err != nil {
		return err
	}

	if err := debugSession.Start(ctx); err != nil {
		return err
	}

	event, err := events.Recv()
	if err != nil {
		return err
	}

	fmt.Printf("debug event %d: state %d\n", event.Sequence, event.Snapshot.State)

	return nil
}
```

See the client documentation for breakpoints, stepping, inspection, and debug
event handling.

## Encoded output

Wire preserves Ferret's encoded result boundary. `client.Output` contains the
encoded `Content` bytes and their `ContentType`; it does not expose Ferret
runtime values directly. Interpret or decode the bytes according to the
reported content type.

## Documentation

- [Client usage and lifecycle](docs/client.md)
- [Client architecture](docs/client-architecture.md)
- [Wire architecture](docs/architecture.md)
- [Protocol contract](docs/architecture.md#protocol-contract)

## Development

The [Makefile](Makefile) is the canonical development interface:

```sh
make build
make test
make test-race
make fmt
make check-fmt
make check-tidy
make vet
make generate
make check-generate
make proto-lint
make proto-breaking BUF_BREAKING_AGAINST=.git#branch=main
```
