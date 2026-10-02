package ferret_test

import (
	"slices"
	"testing"

	"github.com/MontFerret/api"
)

func TestRuntimeVersionRoundTrip(t *testing.T) {
	h := newHarness(t)

	local, err := h.hosted.Version(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	remote, err := h.runtime.Version(h.ctx)
	if err != nil || remote != local {
		t.Fatalf("remote Version=%q, %v; native=%q", remote, err, local)
	}

	if err := h.closeWire(); err != nil {
		t.Fatal(err)
	}

	remote, err = h.runtime.Version(h.ctx)
	if err != nil || remote != local {
		t.Fatalf("cached Version after Wire cleanup=%q, %v; native=%q", remote, err, local)
	}
}

func TestPlanParametersRoundTrip(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, content := range []string{"RETURN 1", "RETURN [@second, @first, @second]"} {
			t.Run(map[bool]string{false: "normal/", true: "debug/"}[debug]+content, func(t *testing.T) {
				h := newHarness(t)
				localCompile, remoteCompile := h.hosted.Compile, h.runtime.Compile

				if debug {
					localCompile, remoteCompile = h.hosted.CompileDebug, h.runtime.CompileDebug
				}

				src := api.NewSource("metadata.fql", content)

				local, err := localCompile(h.ctx, src, api.WithOptimizationLevel(api.OptimizationNone))
				if err != nil {
					t.Fatal(err)
				}

				h.own(local)

				remote, err := remoteCompile(h.ctx, src, api.WithOptimizationLevel(api.OptimizationNone))
				if err != nil {
					t.Fatal(err)
				}

				h.own(remote)

				want, err := local.Params(h.ctx)
				if err != nil {
					t.Fatal(err)
				}

				got, err := remote.Params(h.ctx)
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("remote Params=%v, %v; native=%v", got, err, want)
				}

				if len(got) != 0 {
					got[0] = "caller mutation"
				}

				again, err := remote.Params(h.ctx)
				if err != nil || !slices.Equal(again, want) {
					t.Fatalf("remote Params after mutation=%v, %v; native=%v", again, err, want)
				}
			})
		}
	}
}
