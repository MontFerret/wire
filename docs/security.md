# Security configuration

Wire exposes the host's configured runtime across an RPC boundary. The host
chooses which capabilities are safe to expose, where to listen, certificate
identities and trust roots, and authentication/authorization policy. Construction
never opens a listener. Explicit `Run` creates TCP transport at the supplied
address; `Serve` accepts a caller-created listener. Both use identical constructor
credentials and middleware. gRPC closes accepted listeners when serving returns.

Without configured transport credentials there is no encryption or authentication,
including on loopback. Configured credentials do not fall back to plaintext.
Wire does not load or generate certificates, validate JWTs/API keys, or add
credentials to protobuf messages or UAPI interfaces.

## TLS host and client

These examples share endpoint `127.0.0.1:50051`. The host supplies a server
certificate with DNS SAN `wire.test`, signed by the CA in `serverRoots`.
Certificate loading and lifecycle are application responsibilities.

```go
func serveTLS(ctx context.Context, hostRuntime api.Runtime, serverCertificate tls.Certificate) error {
    transport := credentials.NewTLS(&tls.Config{
        MinVersion:   tls.VersionTLS12,
        Certificates: []tls.Certificate{serverCertificate},
    })
    srv, err := server.New(hostRuntime, server.WithTransportCredentials(transport))
    if err != nil {
        return err
    }
    return srv.Run(ctx, "127.0.0.1:50051")
}
```

The matching client verifies both the root and certificate identity:

```go
func runTLS(ctx context.Context, serverRoots *x509.CertPool) (out *api.Output, err error) {
    conn, err := grpc.NewClient(
        "127.0.0.1:50051",
        grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
            MinVersion: tls.VersionTLS12,
            RootCAs:    serverRoots,
            ServerName: "wire.test",
        })),
    )
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, conn.Close()) }()
    remote, err := client.New(ctx, conn)
    if err != nil {
        return nil, err
    }
    defer func() { err = errors.Join(err, remote.Close()) }()
    return remote.Run(ctx, api.NewAnonymousSource("RETURN 1"))
}
```

`grpc.NewClient` creates transport configuration without connection I/O.
`client.New` performs an actual Connect handshake. Never treat successful transport
construction as proof that TLS or authentication succeeded. Do not disable server
certificate verification.

## Mutual TLS

Use the same endpoint, server identity, and `serverRoots`. The client supplies a
certificate with client-authentication usage, signed by the CA in `clientRoots`.
Configure the host's TLS policy as:

```go
serverTLS := &tls.Config{
    MinVersion:   tls.VersionTLS12,
    Certificates: []tls.Certificate{serverCertificate},
    ClientAuth:   tls.RequireAndVerifyClientCert,
    ClientCAs:    clientRoots,
}
srv, err := server.New(hostRuntime,
    server.WithTransportCredentials(credentials.NewTLS(serverTLS)),
)
if err != nil {
    return err
}
return srv.Run(ctx, "127.0.0.1:50051")
```

The matching client configuration is:

```go
clientTLS := &tls.Config{
    MinVersion:   tls.VersionTLS12,
    RootCAs:      serverRoots,
    ServerName:   "wire.test",
    Certificates: []tls.Certificate{clientCertificate},
}
conn, err := grpc.NewClient("127.0.0.1:50051",
    grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
)
// Check err, then call client.New(ctx, conn) and manage logical/transport cleanup.
```

Host middleware can inspect `peer.FromContext(ctx)` and
`credentials.TLSInfo.State.VerifiedChains`. Certificate verification authenticates
peers; deciding what each peer may do remains host authorization policy.

## TLS plus host token middleware

The following is host application code, not Wire authentication APIs. The host
supplies `validateToken`, including its own issuance, expiry, and authorization
rules. Failures do not disclose token values or validator error text.

```go
func tokenInterceptors(validateToken func(context.Context, string) error) (
    grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor,
) {
    authenticate := func(ctx context.Context) error {
        values := metadata.ValueFromIncomingContext(ctx, "authorization")
        if len(values) != 1 {
            return status.Error(codes.Unauthenticated, "authentication required")
        }
        token, ok := strings.CutPrefix(values[0], "Bearer ")
        if !ok || token == "" || validateToken(ctx, token) != nil {
            return status.Error(codes.Unauthenticated, "authentication required")
        }
        return nil
    }
    unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
        if err := authenticate(ctx); err != nil {
            return nil, err
        }
        return next(ctx, req)
    }
    stream := func(host any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
        if err := authenticate(ss.Context()); err != nil {
            return err
        }
        return next(host, ss)
    }
    return unary, stream
}
```

Configure both RPC types with a server certificate. Mutual TLS can be added
using the client-certificate policy above:

```go
serverTLS := &tls.Config{
    MinVersion:   tls.VersionTLS12,
    Certificates: []tls.Certificate{serverCertificate},
}
unary, stream := tokenInterceptors(validateToken)
srv, err := server.New(hostRuntime,
    server.WithTransportCredentials(credentials.NewTLS(serverTLS)),
    server.WithUnaryInterceptors(unary),
    server.WithStreamInterceptors(stream),
)
if err != nil {
    return err
}
return srv.Run(ctx, "127.0.0.1:50051", server.WithShutdownTimeout(10*time.Second))
```

Use connection-level client `PerRPCCredentials` so each unary operation and new
stream carries the token, including calls after `client.New` and detached cleanup:

```go
type bearerToken string

func (token bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
    return map[string]string{"authorization": "Bearer " + string(token)}, nil
}
func (bearerToken) RequireTransportSecurity() bool { return true }

clientTLS := &tls.Config{
    MinVersion: tls.VersionTLS12,
    RootCAs:    serverRoots,
    ServerName: "wire.test",
}
// Add the client certificate from the mTLS example when the host requires it.
conn, err := grpc.NewClient("127.0.0.1:50051",
    grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
    grpc.WithPerRPCCredentials(bearerToken(hostIssuedToken)),
)
// Check err, then client.New(ctx, conn), query execution, and ordered cleanup.
```

A token attached only to the constructor context does not authenticate subsequent
calls. Authentication hooks must cover unary and streaming RPCs across all services,
including operation calls, execution watches, and debugger command/watch streams.
Hosts can compute or refresh per-call credentials in their credential provider.

## Middleware and trust boundaries

Repeated interceptor options append in registration order. Wire recovery is the
outermost wrapper, followed by host middleware, then the handler. Host middleware
can reject requests before handler resource allocation or runtime work. Ordinary
host authentication/authorization statuses pass through unchanged; invocation
panics receive Wire's existing sanitized internal status. Captured interceptor
lists are copied, but middleware closures must be safe for concurrent RPCs.
Unary replacement contexts and wrapped stream contexts reach the next handler;
incoming metadata and peer authentication information remain available.

Configured message and resource limits still apply with middleware installed.
Do not use authentication as a substitute for runtime policy or resource limits.

**Authentication is not tenant isolation.** Wire's logical connection/resource
ownership is not bound to a middleware principal. Do not assume different
validated users are isolated by token verification alone.

Stream middleware runs at establishment. An already-open stream does not acquire
automatic token revocation or expiry enforcement. Those policies, including
cancellation of established streams when required, belong to the host.

`Run` closes its listener and manages shutdown with a default 30-second budget,
starting when shutdown begins. An earlier explicit deadline shortens it; later
callers cannot extend it. Timeout forces transport shutdown and returns an error
matching `context.DeadlineExceeded`, joined with other known failures. Hosted
cleanup may remain pending under its original owner; subsequent `Shutdown` calls
observe its retained settlement result. Successful return means managed cleanup
settled. Wire never closes the borrowed host runtime. See the
[serving lifecycle](architecture.md#public-serving-lifecycle).
