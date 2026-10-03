package server

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type nilCredentials struct {
	credentials.TransportCredentials
}

func TestSecurityOptionsValidateAndCopyRegistration(t *testing.T) {
	var typedNil *nilCredentials
	for _, option := range []Option{WithTransportCredentials(nil), WithTransportCredentials(typedNil), WithUnaryInterceptors(nil), WithStreamInterceptors(nil)} {
		if _, err := New(&managedRuntime{}, option); err == nil {
			t.Fatal("invalid security option accepted")
		}
	}

	first, second := credentials.NewTLS(&tls.Config{ServerName: "first"}), credentials.NewTLS(&tls.Config{ServerName: "second"})
	cfg := config{}
	for _, option := range []Option{WithTransportCredentials(first), WithTransportCredentials(second), WithUnaryInterceptors(), WithStreamInterceptors()} {
		if err := option.apply(&cfg); err != nil {
			t.Fatal(err)
		}
	}

	if cfg.credentials != second || len(cfg.unary) != 0 || len(cfg.stream) != 0 {
		t.Fatal("scalar/list option conventions changed")
	}

	unary := []grpc.UnaryServerInterceptor{func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		return "original", nil
	}}
	stream := []grpc.StreamServerInterceptor{func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error { return nil }}
	u, s := WithUnaryInterceptors(unary...), WithStreamInterceptors(stream...)
	unary[0], stream[0] = nil, nil
	for range 2 {
		cfg := config{}
		for _, option := range []Option{u, u, s, s} {
			if err := option.apply(&cfg); err != nil {
				t.Fatal(err)
			}
		}

		if len(cfg.unary) != 2 || len(cfg.stream) != 2 {
			t.Fatal("registrations did not append")
		}

		response, err := cfg.unary[0](context.Background(), nil, nil, nil)
		if err != nil || response != "original" {
			t.Fatal("caller mutation changed captured middleware")
		}

		cfg.unary[0], cfg.stream[0] = nil, nil
	}

	s1 := newManagedServer(t, &managedRuntime{}, u, s)

	s2 := newManagedServer(t, &managedRuntime{}, u, s)
	if s1 == s2 {
		t.Fatal("option reuse reused a server")
	}
}
func TestRunOptionsUseLastPositiveTimeout(t *testing.T) {
	cfg := runConfig{shutdownTimeout: 30 * time.Second}
	for _, option := range []RunOption{WithShutdownTimeout(time.Second), WithShutdownTimeout(2 * time.Second)} {
		if err := option.applyRun(&cfg); err != nil {
			t.Fatal(err)
		}
	}

	if cfg.shutdownTimeout != 2*time.Second {
		t.Fatal(cfg.shutdownTimeout)
	}
}
