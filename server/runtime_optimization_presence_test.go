package server_test

import (
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
)

func TestRuntimeOptimizationPresenceRoundTrip(t *testing.T) {
	hosted := &contractRuntime{}
	env := newIntegrationEnv(t, hosted)
	remote, err := client.NewRuntime(testContext(t), env.conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := remote.Close(); err != nil {
			t.Error(err)
		}
	})

	for _, debug := range []bool{false, true} {
		compile := remote.Compile
		if debug {
			compile = remote.CompileDebug
		}

		for _, present := range []bool{false, true} {
			var options []api.PlanOption
			if present {
				options = append(options, api.WithOptimizationLevel(api.OptimizationNone))
			}

			plan, err := compile(testContext(t), api.Source{Content: "RETURN 1"}, options...)
			if err != nil {
				t.Fatal(err)
			}

			if err := plan.Close(); err != nil {
				t.Fatal(err)
			}

			lowLevel, err := env.client.Compile(testContext(t), api.Source{Content: "RETURN 1"}, client.CompileOptions{
				Debuggable:           debug,
				OptimizationLevel:    api.OptimizationNone,
				HasOptimizationLevel: present,
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := lowLevel.Close(testContext(t)); err != nil {
				t.Fatal(err)
			}
		}
	}

	hosted.mu.Lock()
	defer hosted.mu.Unlock()
	for i, present := range []bool{false, false, true, true, false, false, true, true} {
		got := hosted.compileLevels[i]
		if got.hasOptimizationLevel != present || got.optimizationLevel != api.OptimizationNone {
			t.Fatalf("compile %d optimization presence = %+v, want present=%v", i, got, present)
		}
	}
}
