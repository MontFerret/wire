package server_test

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
)

func BenchmarkRuntimeConnectClose(b *testing.B) {
	env := newIntegrationEnv(b, &contractRuntime{})
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		remote, err := client.From(ctx, env.conn)
		if err != nil {
			b.Fatal(err)
		}

		if err := remote.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPlanCompileClose(b *testing.B) {
	for _, debug := range []bool{false, true} {
		name := "normal"

		if debug {
			name = "debug"
		}

		b.Run(name, func(b *testing.B) {
			runtime := &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
				return &contractPlan{params: []string{"input", "other"}}, nil
			}}
			env := newIntegrationEnv(b, runtime)
			ctx := b.Context()

			remote, err := client.From(ctx, env.conn)
			if err != nil {
				b.Fatal(err)
			}

			b.Cleanup(func() {
				if err := remote.Close(); err != nil {
					b.Error(err)
				}
			})
			compile := remote.Compile

			if debug {
				compile = remote.CompileDebug
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				plan, err := compile(ctx, api.NewAnonymousSource("RETURN [@input, @other]"))
				if err != nil {
					b.Fatal(err)
				}

				if err := plan.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHostInterceptorChains(b *testing.B) {
	for _, count := range []int{0, 1, 4} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			unary := make([]grpc.UnaryServerInterceptor, count)
			stream := make([]grpc.StreamServerInterceptor, count)
			for i := range unary {
				unary[i] = func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					return next(ctx, request)
				}
				stream[i] = func(host any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
					return next(host, ss)
				}
			}

			env := newIntegrationEnv(b, &contractRuntime{}, server.WithUnaryInterceptors(unary...), server.WithStreamInterceptors(stream...))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				remote, err := client.From(b.Context(), env.conn)
				if err != nil {
					b.Fatal(err)
				}

				if err := remote.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
