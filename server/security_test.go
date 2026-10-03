package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/test/securityfixture"
)

type (
	middlewareKey struct{}
	contextStream struct {
		grpc.ServerStream
		ctx context.Context
	}
	contextCredentials struct{}
	mutableToken       struct{ value atomic.Value }
)

func (s contextStream) Context() context.Context { return s.ctx }
func (contextCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"observed": "metadata"}, nil
}
func (contextCredentials) RequireTransportSecurity() bool { return false }
func (m *mutableToken) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return securityfixture.Token(m.value.Load().(string)).GetRequestMetadata(ctx, uri...)
}
func (*mutableToken) RequireTransportSecurity() bool { return true }

func startSecurityServer(t testing.TB, s *Server, managed bool) net.Listener {
	t.Helper()

	if managed {
		return startManaged(t, s).listener
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	ready := make(chan struct{})
	observed := &readyListener{Listener: listener, ready: ready}
	result := make(chan error, 1)
	go func() { result <- s.Serve(context.Background(), observed) }()
	managedAwait(t, ready)
	t.Cleanup(func() {
		if err := s.Shutdown(managedContext(t)); err != nil {
			t.Error(err)
		}

		if err := managedAwait(t, result); err != nil {
			t.Error(err)
		}
	})

	return observed
}

func TestTransportCredentialsTLSAndMTLS(t *testing.T) {
	certs := securityfixture.NewCertificates(t)
	for _, managed := range []bool{false, true} {
		for _, mutual := range []bool{false, true} {
			cases := []string{"valid", "untrusted server", "wrong identity", "plaintext"}

			if mutual {
				cases = append(cases, "missing client", "untrusted client")
			}

			for _, test := range cases {
				t.Run(fmt.Sprintf("managed=%v/mutual=%v/%s", managed, mutual, test), func(t *testing.T) {
					var unaryPeer, streamPeer atomic.Int64
					inspect := func(ctx context.Context) error {
						p, ok := peer.FromContext(ctx)
						if !ok {
							return status.Error(codes.Internal, "peer missing")
						}

						info, ok := p.AuthInfo.(credentials.TLSInfo)
						if !ok || !info.State.HandshakeComplete || (mutual && len(info.State.VerifiedChains) == 0) {
							return status.Error(codes.Internal, "verified TLS peer missing")
						}

						return nil
					}
					s := newManagedServer(t, &managedRuntime{}, WithTransportCredentials(credentials.NewTLS(certs.ServerConfig(mutual))),
						WithUnaryInterceptors(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
							if err := inspect(ctx); err != nil {
								return nil, err
							}

							unaryPeer.Add(1)

							return next(ctx, req)
						}), WithStreamInterceptors(func(host any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
							if err := inspect(stream.Context()); err != nil {
								return err
							}

							streamPeer.Add(1)

							return next(host, stream)
						}))
					listener := startSecurityServer(t, s, managed)
					cfg := certs.ClientConfig(mutual)
					switch test {
					case "untrusted server":
						cfg.RootCAs = certs.OtherRoots
					case "wrong identity":
						cfg.ServerName = "other.test"
					case "missing client":
						cfg.Certificates = nil
					case "untrusted client":
						cfg.Certificates = []tls.Certificate{certs.UntrustedClient}
					}

					transport := credentials.TransportCredentials(credentials.NewTLS(cfg))

					if test == "plaintext" {
						transport = insecure.NewCredentials()
					}

					conn := managedClient(t, listener, grpc.WithTransportCredentials(transport))
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					remote, err := client.From(ctx, conn)

					if test != "valid" {
						if err == nil {
							if closeErr := remote.Close(); closeErr != nil {
								t.Error(closeErr)
							}

							t.Fatal("security failure accepted")
						}

						if unaryPeer.Load() != 0 || streamPeer.Load() != 0 {
							t.Fatal("untrusted transport reached middleware")
						}

						return
					}

					if err != nil {
						t.Fatal(err)
					}

					if _, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1")); err != nil {
						t.Fatal(err)
					}

					if err := remote.Close(); err != nil {
						t.Fatal(err)
					}

					if unaryPeer.Load() == 0 || streamPeer.Load() == 0 {
						t.Fatal("authenticated peer was unavailable to middleware")
					}
				})
			}
		}
	}
}

func invokeRegisteredRPC(t testing.TB, conn *grpc.ClientConn, service string, method grpc.MethodInfo) error {
	t.Helper()
	ctx := managedContext(t)

	full := "/" + service + "/" + method.Name
	if !method.IsServerStream && !method.IsClientStream {
		return conn.Invoke(ctx, full, &wirev1.ConnectRequest{}, &wirev1.ConnectRequest{})
	}

	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: method.IsServerStream, ClientStreams: method.IsClientStream}, full)
	if err != nil {
		return err
	}

	if err := stream.SendMsg(&wirev1.ConnectRequest{}); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	if err := stream.CloseSend(); err != nil {
		return err
	}

	return stream.RecvMsg(&wirev1.ConnectRequest{})
}

func TestHostInterceptorsRejectOrRecoverAcrossEveryService(t *testing.T) {
	for _, behavior := range []string{"unauthenticated", "permission denied", "panic"} {
		t.Run(behavior, func(t *testing.T) {
			failure := func() error {
				if behavior == "panic" {
					panic("panic-secret-test-token")
				}

				code := codes.Unauthenticated

				if behavior == "permission denied" {
					code = codes.PermissionDenied
				}

				return status.Error(code, "host rejected request")
			}
			runtime := &managedRuntime{}
			limits := DefaultLimits()
			limits.MaxConnections = 1
			s := newManagedServer(t, runtime, WithLimits(limits), WithUnaryInterceptors(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
				return nil, failure()
			}), WithStreamInterceptors(func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error { return failure() }))
			running := startManaged(t, s)
			conn := managedClient(t, running.listener)
			for service, info := range s.grpcServer.GetServiceInfo() {
				for _, method := range info.Methods {
					err := invokeRegisteredRPC(t, conn, service, method)
					code := codes.Unauthenticated

					if behavior == "permission denied" {
						code = codes.PermissionDenied
					}

					if behavior == "panic" {
						code = codes.Internal
					}

					if status.Code(err) != code {
						t.Fatalf("%s/%s: %v", service, method.Name, err)
					}

					if behavior != "panic" && status.Convert(err).Message() != "host rejected request" {
						t.Fatal("host status rewritten")
					}

					if strings.Contains(fmt.Sprint(status.Convert(err).Proto()), "panic-secret") {
						t.Fatal("panic disclosed")
					}
				}
			}

			if runtime.calls.Load() != 0 {
				t.Fatal("rejected request reached host runtime")
			}

			connection, err := s.connections.Open()
			if err != nil {
				t.Fatalf("rejected requests allocated scopes: %v", err)
			}

			if err := s.connections.CloseConnection(managedContext(t), connection.ID()); err != nil {
				t.Fatal(err)
			}

			running.cancel()

			if err := managedAwait(t, running.result); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHostInterceptorsPreserveOrderAndRequestContexts(t *testing.T) {
	var mu sync.Mutex
	trace := map[string][]string{}
	record := func(method, entry string) { mu.Lock(); trace[method] = append(trace[method], entry); mu.Unlock() }
	streamFinished := make(chan struct{})
	unary := func(name, expected, value string) grpc.UnaryServerInterceptor {
		return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if expected != "" && ctx.Value(middlewareKey{}) != expected {
				return nil, status.Error(codes.Internal, "unary context lost")
			}

			if info.FullMethod == wirev1.PlanService_Compile_FullMethodName {
				record("unary", name+" in")
				defer record("unary", name+" out")
			}

			return next(context.WithValue(ctx, middlewareKey{}, value), req)
		}
	}
	stream := func(name, expected, value string) grpc.StreamServerInterceptor {
		return func(host any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
			if expected != "" && ss.Context().Value(middlewareKey{}) != expected {
				return status.Error(codes.Internal, "stream context lost")
			}

			if info.FullMethod == wirev1.RuntimeService_Connect_FullMethodName {
				record("stream", name+" in")
				defer record("stream", name+" out")

				if name == "first" {
					defer close(streamFinished)
				}
			}

			return next(host, contextStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), middlewareKey{}, value)})
		}
	}
	check := func(ctx context.Context, value string) error {
		if ctx.Value(middlewareKey{}) != value || len(metadata.ValueFromIncomingContext(ctx, "observed")) != 1 {
			return status.Error(codes.Internal, "middleware context or metadata lost")
		}

		if _, ok := peer.FromContext(ctx); !ok {
			return status.Error(codes.Internal, "peer lost")
		}

		return nil
	}
	runtime := &managedRuntime{plan: &managedPlan{}, version: func(ctx context.Context) (api.Version, error) { return "middleware", check(ctx, "stream") }}
	runtime.compile = func(ctx context.Context) (api.Plan, error) { return runtime.plan, check(ctx, "unary") }
	s := newManagedServer(t, runtime, WithUnaryInterceptors(unary("first", "", "outer")), WithUnaryInterceptors(unary("second", "outer", "unary")), WithStreamInterceptors(stream("first", "", "outer")), WithStreamInterceptors(stream("second", "outer", "stream")))
	running := startManaged(t, s)
	conn := managedClient(t, running.listener, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(contextCredentials{}))

	remote, err := client.From(managedContext(t), conn)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := remote.Compile(managedContext(t), api.NewAnonymousSource("RETURN 1"))
	if err != nil {
		t.Fatal(err)
	}

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}

	managedAwait(t, streamFinished)
	mu.Lock()
	defer mu.Unlock()
	for _, kind := range []string{"unary", "stream"} {
		if !reflect.DeepEqual(trace[kind], []string{"first in", "second in", "second out", "first out"}) {
			t.Fatalf("%s order: %v", kind, trace[kind])
		}
	}
}

func TestTokenAuthenticationCoversOperationsAfterConnect(t *testing.T) {
	certs := securityfixture.NewCertificates(t)
	for _, managed := range []bool{false, true} {
		t.Run(fmt.Sprint(managed), func(t *testing.T) {
			runtime := &managedRuntime{}
			s := newManagedServer(t, runtime, WithTransportCredentials(credentials.NewTLS(certs.ServerConfig(false))), WithUnaryInterceptors(securityfixture.Unary), WithStreamInterceptors(securityfixture.Stream))
			listener := startSecurityServer(t, s, managed)
			token := &mutableToken{}
			token.value.Store("Bearer test-token")
			conn := managedClient(t, listener, grpc.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(false))), grpc.WithPerRPCCredentials(token))

			connect, err := wirev1.NewRuntimeServiceClient(conn).Connect(managedContext(t), &wirev1.ConnectRequest{})
			if err != nil {
				t.Fatal(err)
			}

			if _, err := connect.Recv(); err != nil {
				t.Fatal(err)
			}

			before := runtime.calls.Load()
			for _, invalid := range []string{"", "malformed", "Bearer invalid"} {
				token.value.Store(invalid)
				for service, info := range s.grpcServer.GetServiceInfo() {
					for _, method := range info.Methods {
						if err := invokeRegisteredRPC(t, conn, service, method); status.Code(err) != codes.Unauthenticated {
							t.Fatalf("%s/%s bypassed token authentication: %v", service, method.Name, err)
						}
					}
				}
			}

			if runtime.calls.Load() != before {
				t.Fatal("unauthenticated operation reached runtime")
			}

			token.value.Store("Bearer test-token")

			remote, err := client.From(managedContext(t), conn)
			if err != nil {
				t.Fatal(err)
			}

			output, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1"))
			if err != nil || string(output.Content) != "[1]" {
				t.Fatalf("accepted operation: output=%v err=%v", output, err)
			}

			if err := remote.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHostMiddlewarePreservesMessageAndResourceLimits(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxConnections = 1
	limits.MaxPlansPerConnection = 1
	limits.MaxInboundMessageBytes = 256
	limits.MaxOutboundMessageBytes = 256
	runtime := &managedRuntime{plan: &managedPlan{}, run: func(context.Context) (*api.Output, error) {
		return &api.Output{ContentType: "application/json", Content: []byte(strings.Repeat("x", 1024))}, nil
	}}
	s := newManagedServer(t, runtime, WithLimits(limits), WithUnaryInterceptors(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		return next(ctx, req)
	}), WithStreamInterceptors(func(host any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		return next(host, ss)
	}))
	running := startManaged(t, s)
	conn := managedClient(t, running.listener)

	stream, err := wirev1.NewRuntimeServiceClient(conn).Connect(managedContext(t), &wirev1.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}

	handshake, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}

	other, err := wirev1.NewRuntimeServiceClient(conn).Connect(managedContext(t), &wirev1.ConnectRequest{})
	if err == nil {
		_, err = other.Recv()
	}

	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("connection limit bypassed: %v", err)
	}

	plans := wirev1.NewPlanServiceClient(conn)

	request := &wirev1.CompileRequest{ConnectionId: handshake.ConnectionId, Source: &wirev1.Source{Content: "RETURN 1"}}
	if _, err := plans.Compile(managedContext(t), request); err != nil {
		t.Fatal(err)
	}

	if _, err := plans.Compile(managedContext(t), request); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("plan limit bypassed: %v", err)
	}

	request.Source.Content = strings.Repeat("x", 1024)
	if _, err := plans.Compile(managedContext(t), request); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("message limit bypassed: %v", err)
	}

	if _, err := wirev1.NewRuntimeServiceClient(conn).CloseConnection(managedContext(t), &wirev1.CloseConnectionRequest{ConnectionId: handshake.ConnectionId}); err != nil {
		t.Fatal(err)
	}

	remote, err := client.From(managedContext(t), conn)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := remote.Run(managedContext(t), api.NewAnonymousSource("RETURN 1")); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("outbound message limit bypassed: %v", err)
	}

	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
}
