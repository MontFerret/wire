# Client Handles

The handwritten `client` package is a domain facade over the generated gRPC
clients. It owns one logical Wire connection while borrowing the caller's
`grpc.ClientConnInterface`.

## Resource model

Remote resources are exposed as opaque, typed handles:

```text
Client
└── Plan
    ├── Execution
    └── DebugSession
```

`Client` compiles plans. A `Plan` executes its compiled program and creates
debug sessions. Execution and debugger operations live on their respective
handles. The protocol still carries connection, plan, execution, and
debug-session IDs, but the facade retains and propagates them privately.
Callers cannot manually combine a handle with another client's connection.

```go
plan, err := wireClient.Compile(ctx, source, client.CompileOptions{})
if err != nil {
	return err
}
defer func() {
	if closeErr := plan.Close(context.Background()); closeErr != nil {
		log.Printf("close plan: %v", closeErr)
	}
}()

execution, err := plan.Execute(ctx, parameters, client.ExecuteOptions{})
if err != nil {
	return err
}
defer func() {
	if closeErr := execution.Close(context.Background()); closeErr != nil {
		log.Printf("close execution: %v", closeErr)
	}
}()

events, err := execution.Watch(ctx)
```

Plan metadata is immutable. `Parameters` returns a defensive copy, and
`Debuggable` reports the compile option reflected by the server.

## Snapshots and events

Handles represent identity, ownership, and operations; they are not mutable
state snapshots. Execution and debug state crosses the facade through
`ExecutionSnapshot` and `DebugSessionSnapshot` values on ordered watch events.
These snapshots preserve Ferret's encoded output and structured debugger data
without exposing generated protobuf messages.

Execution and debugger command methods return errors only. Their state changes
are observed through `Execution.Watch` or `DebugSession.Watch`. A watch sends
the server's latest published event first when one exists, then ordered changes
through one terminal event. A newly created debug session has no published
event until debugging starts; the client does not fabricate a pre-start event.

Watch streams are tied to both the operation context and the logical Client
lifecycle. A watch opened before resource closure remains able to receive the
server's terminal event. New watches are rejected after the handle or an
ancestor starts closing.

## Closing resources

`Plan`, `Execution`, and `DebugSession` expose `Close(context.Context) error`.
Close maps to the corresponding protocol release operation; it is never driven
by finalizers or garbage collection.

The first Close commits teardown exactly once. Concurrent and repeated callers
wait for the same retained release result. A waiter's context can expire
without cancelling the committed cleanup, and a later call can still observe
the retained result. Release failures are retained rather than hidden or
retried. Once close begins, the handle rejects new operations with an error
matching `client.ErrClosed`.

Closing a Client or Plan owns cleanup of its descendants. Descendant operations
are rejected as soon as ancestor closure begins. A descendant first closed
after ancestor cleanup begins observes the ancestor's retained result rather
than issuing a duplicate release. Close children before parents when reporting
each resource's direct release result matters; normal `defer` ordering provides
this naturally.

`DebugSession.Stop` and `DebugSession.Close` are intentionally distinct. Stop
terminates debugger execution without releasing the remote ID; Close commits
termination and resource release.

## Facade responsibilities

The client package is limited to:

- logical Connect lifecycle;
- typed plan, execution, debugger, and event operations;
- private connection and resource-ID propagation;
- explicit parameter conversion;
- immutable domain snapshots and structured error mapping;
- hiding protobuf and gRPC ceremony without hiding protocol concepts.

Closing the Client never closes the caller-owned gRPC connection. The facade
does not construct engines or transports, add one-shot compile/execute/wait
workflows, or duplicate Ferret runtime semantics.
