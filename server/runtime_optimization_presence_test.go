package server_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
)

func TestClientOptimizationPresenceRoundTrip(t *testing.T) {
	hosted := &contractRuntime{}
	env := newIntegrationEnv(t, hosted)

	for _, debug := range []bool{false, true} {
		for _, test := range []struct {
			name    string
			present bool
			level   api.OptimizationLevel
		}{
			{name: "omitted"},
			{name: "none", present: true, level: api.OptimizationNone},
			{name: "basic", present: true, level: api.OptimizationBasic},
			{name: "full", present: true, level: api.OptimizationFull},
			{name: "aggressive", present: true, level: api.OptimizationAggressive},
		} {
			t.Run(map[bool]string{false: "normal/", true: "debug/"}[debug]+test.name, func(t *testing.T) {
				var options []api.PlanOption

				if test.present {
					options = append(options, api.WithOptimizationLevel(test.level))
				}

				compile := env.client.Compile
				if debug {
					compile = env.client.CompileDebug
				}

				plan, err := compile(testContext(t), api.Source{Content: "RETURN 1"}, options...)
				if err != nil {
					t.Fatal(err)
				}

				if err := plan.Close(); err != nil {
					t.Fatal(err)
				}

				hosted.mu.Lock()
				defer hosted.mu.Unlock()

				levels := hosted.compileLevels[len(hosted.compileLevels)-1:]

				for _, got := range levels {
					if got.hasOptimizationLevel != test.present || got.optimizationLevel != test.level {
						t.Fatalf("optimization = %+v, want present=%v level=%v", got, test.present, test.level)
					}
				}
			})
		}
	}
}

func TestCompileOptionsApplyOnceBeforeDispatch(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, outcome := range []string{"success", "invalid level", "callback errors", "callback cancellation", "already cancelled"} {
			name := "Runtime/" + map[bool]string{false: "normal/", true: "debug/"}[debug] + outcome
			t.Run(name, func(t *testing.T) {
				hosted := &contractRuntime{}
				env := newIntegrationEnv(t, hosted)
				gate := &allocationResponseGate{ClientConnInterface: env.conn, calls: make(map[string]int)}
				remote, err := client.New(testContext(t), gate)
				if err != nil {
					t.Fatal(err)
				}

				t.Cleanup(func() {
					if err := remote.Close(); err != nil {
						t.Error(err)
					}
				})
				compile := func(ctx context.Context, options []api.PlanOption) error {
					compile := remote.Compile
					if debug {
						compile = remote.CompileDebug
					}

					plan, err := compile(ctx, api.Source{Content: "RETURN 1"}, options...)
					if err != nil {
						return err
					}

					return plan.Close()
				}

				ctx, cancel := context.WithCancel(testContext(t))
				defer cancel()

				firstErr, secondErr := errors.New("first option failed"), errors.New("second option failed")
				var order []int
				options := []api.PlanOption{
					func(options api.PlanOptions) error {
						order = append(order, 1)

						if outcome == "callback cancellation" {
							cancel()
						}

						if outcome == "callback errors" {
							return firstErr
						}

						if outcome == "invalid level" {
							return options.SetOptimizationLevel(api.OptimizationLevel(99))
						}

						return options.SetOptimizationLevel(api.OptimizationBasic)
					},
					nil,
					func(options api.PlanOptions) error {
						order = append(order, 2)

						if outcome == "callback errors" {
							return secondErr
						}

						return options.SetOptimizationLevel(api.OptimizationNone)
					},
				}

				if outcome == "already cancelled" {
					cancel()
				}

				err = compile(ctx, options)

				if outcome == "already cancelled" {
					if len(order) != 0 {
						t.Fatalf("cancelled call applied options: %v", order)
					}
				} else if !reflect.DeepEqual(order, []int{1, 2}) {
					t.Fatalf("callbacks did not run once in order: %v", order)
				}

				switch outcome {
				case "success":
					if err != nil {
						t.Fatal(err)
					}
				case "callback cancellation", "already cancelled":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation was lost: %v", err)
					}
				case "callback errors":
					if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
						t.Fatalf("callback errors were not joined: %v", err)
					}
				default:
					if err == nil {
						t.Fatal("invalid optimization was accepted")
					}
				}

				method := wirev1.PlanService_Compile_FullMethodName

				if debug {
					method = wirev1.PlanService_CompileDebug_FullMethodName
				}

				expectedCalls := 0

				if outcome == "success" {
					expectedCalls = 1
				}

				if calls := gate.count(method); calls != expectedCalls {
					t.Fatalf("dispatched %d compile requests, want %d", calls, expectedCalls)
				}

				hosted.mu.Lock()
				defer hosted.mu.Unlock()

				if outcome == "success" {
					if len(hosted.compileLevels) != 1 || !hosted.compileLevels[0].hasOptimizationLevel ||
						hosted.compileLevels[0].optimizationLevel != api.OptimizationNone || hosted.compileDebug[0] != debug {
						t.Fatalf("host did not receive final explicit zero option and debug choice: %+v", hosted.compileLevels)
					}
				} else if len(hosted.compileSources) != 0 {
					t.Fatal("failed local options reached the hosted runtime")
				}
			})
		}
	}
}
