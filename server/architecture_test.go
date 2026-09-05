package server_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"
	"testing"

	wiredebugger "github.com/MontFerret/wire/pkg/debugger"
	"github.com/MontFerret/wire/pkg/execution"
)

func TestPackageDependencyDirection(t *testing.T) {
	root := repositoryRoot(t)
	legacyRuntimePackage := filepath.Join(root, "pkg", "runtime")
	if _, err := os.Stat(legacyRuntimePackage); err == nil {
		t.Errorf("obsolete shared package remains at %s", legacyRuntimePackage)
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".go" {
			t.Errorf("module root contains Go source %s; the root compatibility package must remain absent", entry.Name())
		}
	}

	checks := []struct {
		directory string
		forbidden []string
	}{
		{directory: "client", forbidden: []string{"github.com/MontFerret/ferret", "github.com/MontFerret/wire/server"}},
		{directory: "server", forbidden: []string{"github.com/MontFerret/ferret", "github.com/MontFerret/wire/client"}},
		{directory: "pkg", forbidden: []string{"github.com/MontFerret/ferret", "github.com/MontFerret/wire/client", "github.com/MontFerret/wire/server"}},
	}

	for _, check := range checks {
		err := filepath.WalkDir(filepath.Join(root, check.directory), func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, spec := range parsed.Imports {
				importPath := strings.Trim(spec.Path.Value, "\"")
				for _, forbidden := range check.forbidden {
					if importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/") {
						t.Errorf("%s imports forbidden package %s", path, importPath)
					}
				}
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSharedSnapshotsContainNoResourceIdentity(t *testing.T) {
	for name, snapshot := range map[string]reflect.Type{
		"execution": reflect.TypeFor[execution.Snapshot](),
		"debugger":  reflect.TypeFor[wiredebugger.Snapshot](),
	} {
		for _, field := range []string{"ConnectionID", "PlanID", "SessionID", "ExecutionID", "DebugSessionID"} {
			if _, exists := snapshot.FieldByName(field); exists {
				t.Errorf("%s snapshot exposes server-private field %s", name, field)
			}
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("cannot locate architecture test")
	}

	return filepath.Dir(filepath.Dir(file))
}
