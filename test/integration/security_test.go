package integration_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
	"github.com/MontFerret/wire/test/securityfixture"
)

func TestPublicTLSAuthenticatedRuntimeRoundTrip(t *testing.T) {
	certs := securityfixture.NewCertificates(t)
	hosted := harness.NewRuntimeSpy(harness.RuntimeBehavior{Plan: harness.PlanBehavior{Params: []string{"input"}, Session: func(harness.SessionOptions) harness.SessionBehavior {
		return harness.SessionBehavior{Run: func(context.Context, int) (*api.Output, error) {
			return &api.Output{ContentType: "application/json", Content: []byte("[42]")}, nil
		}}
	}}})

	s, err := server.New(hosted, server.WithTransportCredentials(credentials.NewTLS(certs.ServerConfig(true))), server.WithUnaryInterceptors(securityfixture.Unary), server.WithStreamInterceptors(securityfixture.Stream))
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() { result <- s.Serve(context.Background(), listener) }()
	t.Cleanup(func() {
		if err := s.Shutdown(harness.Context(t)); err != nil {
			t.Error(err)
		}

		if err := harness.Await(t, result); err != nil {
			t.Error(err)
		}
	})

	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(true))), grpc.WithPerRPCCredentials(securityfixture.Token("Bearer test-token")))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})

	remote, err := client.From(harness.Context(t), conn)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := remote.Compile(harness.Context(t), api.NewAnonymousSource("RETURN @input"))
	if err != nil {
		t.Fatal(err)
	}

	session, err := plan.NewSession(harness.Context(t), api.WithParam("input", 42))
	if err != nil {
		t.Fatal(err)
	}

	output, err := session.Run(harness.Context(t))
	if err != nil {
		t.Fatal(err)
	}

	if output.ContentType != "application/json" || string(output.Content) != "[42]" {
		t.Fatalf("encoded output changed: %+v", output)
	}

	for _, resource := range []interface{ Close() error }{session, plan, remote} {
		if err := resource.Close(); err != nil {
			t.Fatal(err)
		}
	}

	hosted.Recorder().AssertClosed(t)
}
