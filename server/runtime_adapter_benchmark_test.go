package server_test

import (
	"errors"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
)

func BenchmarkRuntimeAdapterDurableSession(b *testing.B) {
	env := newIntegrationEnv(b, &contractRuntime{})

	remote, err := client.New(testContext(b), env.conn)
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		if err := remote.Close(); err != nil {
			b.Error(err)
		}
	})

	plan, err := remote.Compile(testContext(b), api.Source{Content: "RETURN 1"})
	if err != nil {
		b.Fatal(err)
	}

	session, err := plan.NewSession(testContext(b))
	if err != nil {
		b.Fatal(err)
	}

	b.Cleanup(func() {
		if err := errors.Join(session.Close(), plan.Close()); err != nil {
			b.Error(err)
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := session.Run(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}
