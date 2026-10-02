package server_test

import (
	"context"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
)

func BenchmarkRuntimeConnectClose(b *testing.B) {
	env := newIntegrationEnv(b, &contractRuntime{})
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		remote, err := client.New(ctx, env.conn)
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

			remote, err := client.New(ctx, env.conn)
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
