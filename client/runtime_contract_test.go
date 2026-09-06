package client_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"google.golang.org/grpc"
)

// The constructor returns the canonical interface, including a nil interface
// on failure, without requiring a Wire resource type in consumer code.
var _ func(context.Context, grpc.ClientConnInterface) (api.Runtime, error) = client.New

func TestNewRejectsMissingTransport(t *testing.T) {
	remote, err := client.New(t.Context(), nil)
	if err == nil || remote != nil {
		t.Fatalf("New(nil) = %v, %v; want nil runtime and an error", remote, err)
	}
}

func TestPublicSurface(t *testing.T) {
	// go test runs in the package directory, including when built with -trimpath.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	var exported []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}

		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		for name := range file.Scope.Objects {
			if ast.IsExported(name) {
				exported = append(exported, name)
			}
		}
	}

	slices.Sort(exported)
	want := []string{"ErrClosed", "ErrExecutionCancelled", "Error", "New"}
	if !slices.Equal(exported, want) {
		t.Fatalf("public client surface = %v, want %v", exported, want)
	}
}
