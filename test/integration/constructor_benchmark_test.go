package integration_test

import (
	"testing"

	"google.golang.org/grpc/credentials"

	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
	"github.com/MontFerret/wire/test/integration/harness"
	"github.com/MontFerret/wire/test/securityfixture"
)

func BenchmarkOwningConstructor(b *testing.B) {
	for _, transport := range []string{"plaintext", "TLS"} {
		b.Run(transport, func(b *testing.B) {
			options := []client.Option{client.WithInsecure()}
			var serverOptions []server.Option

			if transport == "TLS" {
				certs := securityfixture.NewCertificates(b)
				options = []client.Option{client.WithTransportCredentials(credentials.NewTLS(certs.ClientConfig(false)))}
				serverOptions = []server.Option{server.WithTransportCredentials(credentials.NewTLS(certs.ServerConfig(false)))}
			}

			h := harness.New(b, harness.WithOwnedTransport(options...), harness.WithServerOptions(serverOptions...))
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				remote, err := client.New(ctx, h.Endpoint(), options...)
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
