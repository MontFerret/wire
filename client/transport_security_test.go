package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
	"github.com/MontFerret/wire/test/securityfixture"
)

type (
	protectedCredentials struct{ calls atomic.Int64 }
	rotatingCredentials  struct {
		mu        sync.Mutex
		token     string
		methods   map[string][]string
		providers []string
	}
	companionCredentials struct{ state *rotatingCredentials }
)

func (c *protectedCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	c.calls.Add(1)

	return map[string]string{"authorization": "protected-secret"}, nil
}
func (*protectedCredentials) RequireTransportSecurity() bool { return true }

func TestOwningConstructorTransportSecurity(t *testing.T) {
	certs := securityfixture.NewCertificates(t)
	for _, host := range []string{"plaintext", "TLS", "mTLS"} {
		t.Run(host, func(t *testing.T) {
			var serverOptions []server.Option
			initial := []client.Option{client.WithInsecure()}

			if host != "plaintext" {
				serverOptions = append(serverOptions, server.WithTransportCredentials(credentials.NewTLS(certs.ServerConfig(host == "mTLS"))))
				initial = []client.Option{client.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(host == "mTLS")))}
			}

			h := harness.New(t, harness.WithOwnedTransport(initial...), harness.WithServerOptions(serverOptions...))
			type testCase struct {
				name    string
				options []client.Option
				success bool
			}
			var tests []testCase

			if host == "plaintext" {
				tests = []testCase{
					{name: "default rejects plaintext"},
					{name: "explicit plaintext", options: []client.Option{client.WithInsecure()}, success: true},
					{name: "repeated plaintext", options: []client.Option{client.WithInsecure(), client.WithInsecure()}, success: true},
				}
			} else {
				wrongIdentity := certs.ClientConfig(host == "mTLS")
				wrongIdentity.ServerName = "other.test"
				wrongRoots := certs.ClientConfig(host == "mTLS")
				wrongRoots.RootCAs = certs.OtherRoots
				correct := client.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(host == "mTLS")))
				wrong := client.WithTransportCredentials(credentials.NewTLS(wrongIdentity))
				tests = []testCase{
					{name: "system roots reject private CA"},
					{name: "configured trust", options: []client.Option{correct}, success: true},
					{name: "untrusted roots", options: []client.Option{client.WithTransportCredentials(credentials.NewTLS(wrongRoots))}},
					{name: "incorrect identity", options: []client.Option{wrong}},
					{name: "last valid transport wins", options: []client.Option{wrong, correct}, success: true},
					{name: "last invalid policy wins", options: []client.Option{correct, wrong}},
				}

				if host == "mTLS" {
					untrustedClient := certs.ClientConfig(true)
					untrustedClient.Certificates = []tls.Certificate{certs.UntrustedClient}
					tests = append(tests,
						testCase{name: "missing client certificate", options: []client.Option{client.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(false)))}},
						testCase{name: "untrusted client certificate", options: []client.Option{client.WithTransportCredentials(credentials.NewTLS(untrustedClient))}},
					)
				}
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(h.Context(), 2*time.Second)
					defer cancel()

					remote, err := client.New(ctx, h.Endpoint(), test.options...)
					if !test.success {
						if err == nil || remote != nil {
							t.Fatalf("security failure published runtime: %v, %v", remote, err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}

						h.Own(remote)

						if _, err := remote.Run(h.Context(), api.NewAnonymousSource("RETURN 1")); err != nil {
							t.Fatal(err)
						}

						if err := remote.Close(); err != nil {
							t.Fatal(err)
						}
					}

					if _, err := h.Runtime().Run(h.Context(), api.NewAnonymousSource("RETURN 2")); err != nil {
						t.Fatalf("constructor failure damaged independent channel: %v", err)
					}
				})
			}
		})
	}
}

func TestProtectedCredentialsNeverReachPlaintext(t *testing.T) {
	var requests atomic.Int64
	observe := func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		requests.Add(1)

		return next(ctx, request)
	}
	h := harness.New(t, harness.WithOwnedTransport(client.WithInsecure()), harness.WithServerOptions(server.WithUnaryInterceptors(observe)))
	provider := &protectedCredentials{}

	remote, err := client.New(h.Context(), h.Endpoint(), client.WithInsecure(), client.WithPerRPCCredentials(provider))
	if err == nil || remote != nil || provider.calls.Load() != 0 || requests.Load() != 0 {
		t.Fatalf("protected credentials used plaintext: runtime=%v err=%v metadata calls=%d requests=%d", remote, err, provider.calls.Load(), requests.Load())
	}
}

func (c *rotatingCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.providers = append(c.providers, "second")

	return map[string]string{"authorization": c.token, "x-provider": "second"}, nil
}
func (*rotatingCredentials) RequireTransportSecurity() bool { return true }

func (c companionCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	c.state.mu.Lock()
	c.state.providers = append(c.state.providers, "first")
	c.state.mu.Unlock()

	return map[string]string{"x-companion": "present", "x-provider": "first"}, nil
}
func (companionCredentials) RequireTransportSecurity() bool { return true }

func (c *rotatingCredentials) authenticate(ctx context.Context, method string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()

	token := md.Get("authorization")
	if len(token) != 1 || token[0] != c.token || len(md.Get("x-companion")) != 1 || len(md.Get("x-provider")) != 1 || md.Get("x-provider")[0] != "second" {
		return status.Error(codes.Unauthenticated, "authentication required")
	}

	c.methods[method] = append(c.methods[method], token[0])

	return nil
}

func TestOwningCredentialsRefreshForOperationsStreamsAndCleanup(t *testing.T) {
	certs := securityfixture.NewCertificates(t)
	provider := &rotatingCredentials{token: "first-token", methods: make(map[string][]string)}
	unary := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if err := provider.authenticate(ctx, info.FullMethod); err != nil {
			return nil, err
		}

		return next(ctx, request)
	}
	stream := func(host any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if err := provider.authenticate(stream.Context(), info.FullMethod); err != nil {
			return err
		}

		return next(host, stream)
	}
	options := []client.Option{client.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(true))), client.WithPerRPCCredentials(companionCredentials{state: provider}), client.WithPerRPCCredentials(provider)}

	h := harness.New(t, harness.WithOwnedTransport(options...), harness.WithServerOptions(
		server.WithTransportCredentials(credentials.NewTLS(certs.ServerConfig(true))),
		server.WithUnaryInterceptors(unary), server.WithStreamInterceptors(stream),
	))
	if _, err := h.Runtime().Run(h.Context(), api.NewAnonymousSource("RETURN 1")); err != nil {
		t.Fatal(err)
	}

	provider.mu.Lock()
	provider.token = "refreshed-token"
	provider.mu.Unlock()

	plan, err := h.Runtime().CompileDebug(h.Context(), api.NewAnonymousSource("RETURN 2"))
	if err != nil {
		t.Fatal(err)
	}

	h.Own(plan)

	session, err := plan.NewSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(session)

	if _, err := session.Run(h.Context()); err != nil {
		t.Fatal(err)
	}

	debug, err := plan.NewDebugSession(h.Context())
	if err != nil {
		t.Fatal(err)
	}

	h.Own(debug)

	if _, err := debug.Start(h.Context()); err != nil {
		t.Fatal(err)
	}

	if _, err := debug.Continue(h.Context()); err != nil {
		t.Fatal(err)
	}

	for _, resource := range []interface{ Close() error }{debug, session, plan, h.Runtime()} {
		if err := resource.Close(); err != nil {
			t.Fatal(err)
		}
	}

	harness.Await(t, h.TransportClosed())
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for _, method := range []string{
		"/ferret.wire.v1.RuntimeService/Connect",
		"/ferret.wire.v1.RuntimeService/Run",
		"/ferret.wire.v1.PlanService/CompileDebug",
		"/ferret.wire.v1.SessionService/CreateSession",
		"/ferret.wire.v1.ExecutionService/WatchExecution",
		"/ferret.wire.v1.DebugService/RunCommand",
		"/ferret.wire.v1.ExecutionService/ReleaseExecution",
		"/ferret.wire.v1.DebugService/ReleaseDebugSession",
		"/ferret.wire.v1.SessionService/ReleaseSession",
		"/ferret.wire.v1.PlanService/ReleasePlan",
		"/ferret.wire.v1.RuntimeService/CloseConnection",
	} {
		tokens := provider.methods[method]
		if len(tokens) == 0 {
			t.Fatalf("credentials missing on %s", method)
		}

		if method == "/ferret.wire.v1.RuntimeService/CloseConnection" && tokens[0] != "refreshed-token" {
			t.Fatal("cleanup froze startup authentication")
		}
	}

	if len(provider.providers)%2 != 0 {
		t.Fatal("incomplete credential composition")
	}

	for i := 0; i < len(provider.providers); i += 2 {
		if provider.providers[i] != "first" || provider.providers[i+1] != "second" {
			t.Fatal("credential provider registration order changed")
		}
	}
}

func TestOwningAuthenticationRejectionReturnsNilRuntime(t *testing.T) {
	certs := securityfixture.NewCertificates(t)
	h := harness.New(t, harness.WithOwnedTransport(client.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(false))), client.WithPerRPCCredentials(securityfixture.Token("Bearer test-token"))), harness.WithServerOptions(
		server.WithTransportCredentials(credentials.NewTLS(certs.ServerConfig(false))),
		server.WithUnaryInterceptors(securityfixture.Unary), server.WithStreamInterceptors(securityfixture.Stream),
	))

	remote, err := client.New(h.Context(), h.Endpoint(), client.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(false))), client.WithPerRPCCredentials(securityfixture.Token("Bearer wrong-token")))
	if remote != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("authentication rejection changed: %v, %v", remote, err)
	}

	var decoded *client.Error
	if !errors.As(err, &decoded) {
		t.Fatalf("startup lost decoded transport error: %v", err)
	}
}
