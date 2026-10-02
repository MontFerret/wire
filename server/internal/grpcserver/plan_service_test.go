package grpcserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func TestOptimizationLevelMapsPortableValuesAndPreservesRuntimeDefault(t *testing.T) {
	tests := []struct {
		name    string
		options *wirev1.CompileOptions
		want    api.OptimizationLevel
		present bool
	}{
		{name: "missing options"},
		{name: "unspecified", options: &wirev1.CompileOptions{}},
		{name: "none", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_NONE), want: api.OptimizationNone, present: true},
		{name: "basic", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_BASIC), want: api.OptimizationBasic, present: true},
		{name: "full", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_FULL), want: api.OptimizationFull, present: true},
		{name: "aggressive", options: compileOptions(wirev1.OptimizationLevel_OPTIMIZATION_LEVEL_AGGRESSIVE), want: api.OptimizationAggressive, present: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, present, err := optimizationLevel(test.options)
			if err != nil {
				t.Fatal(err)
			}

			if got != test.want || present != test.present {
				t.Fatalf("optimization = (%v, %v), want (%v, %v)", got, present, test.want, test.present)
			}
		})
	}
}

func TestOptimizationLevelRejectsUnknownValue(t *testing.T) {
	_, _, err := optimizationLevel(compileOptions(wirev1.OptimizationLevel(99)))

	var domain *core.DomainError
	if !errors.As(err, &domain) || domain.Kind != core.ErrorKindInvalidRequest {
		t.Fatalf("unexpected invalid optimization result: %v", err)
	}
}

func compileOptions(level wirev1.OptimizationLevel) *wirev1.CompileOptions {
	return &wirev1.CompileOptions{OptimizationLevel: level}
}

type (
	metadataRuntime struct {
		api.Runtime
		plan api.Plan
	}

	metadataPlan struct {
		api.Plan
		params func(context.Context) ([]string, error)
		closed int
	}
)

func (r *metadataRuntime) Compile(context.Context, api.Source, ...api.PlanOption) (api.Plan, error) {
	return r.plan, nil
}

func (r *metadataRuntime) CompileDebug(context.Context, api.Source, ...api.PlanOption) (api.Plan, error) {
	return r.plan, nil
}

func (p *metadataPlan) Params(ctx context.Context) ([]string, error) {
	return p.params(ctx)
}

func (p *metadataPlan) Close() error {
	p.closed++

	return nil
}

func TestCompileMetadataCancellationReclaimsHostedPlan(t *testing.T) {
	for _, debug := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			name := map[bool]string{false: "normal/", true: "debug/"}[debug] + map[bool]string{false: "canceled", true: "deadline"}[deadline]
			t.Run(name, func(t *testing.T) {
				lifetime, finish := context.WithTimeout(t.Context(), 10*time.Second)
				defer finish()
				ctx, cancel := context.WithCancel(lifetime)
				defer cancel()

				if deadline {
					ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()
				}

				connections := core.NewConnectionRegistry(1, core.ResourceLimits{Plans: 1})

				connection, err := connections.Open()
				if err != nil {
					t.Fatal(err)
				}

				hosted := &metadataPlan{params: func(ctx context.Context) ([]string, error) {
					if deadline {
						<-ctx.Done()
					} else {
						cancel()
					}

					return []string{"partial"}, ctx.Err()
				}}
				runtime := &metadataRuntime{plan: hosted}
				service := &PlanService{runtime: runtime, connections: connections}
				id := &wirev1.ConnectionId{Value: string(connection.ID())}

				if debug {
					response, compileErr := service.CompileDebug(ctx, &wirev1.CompileDebugRequest{ConnectionId: id, Source: &wirev1.Source{}})
					if response != nil {
						t.Fatal("metadata cancellation published a debug plan")
					}

					err = compileErr
				} else {
					response, compileErr := service.Compile(ctx, &wirev1.CompileRequest{ConnectionId: id, Source: &wirev1.Source{}})
					if response != nil {
						t.Fatal("metadata cancellation published a normal plan")
					}

					err = compileErr
				}

				want := codes.Canceled

				if deadline {
					want = codes.DeadlineExceeded
				}

				if status.Code(err) != want || hosted.closed != 1 {
					t.Fatalf("metadata cancellation error=%v, close count=%d", err, hosted.closed)
				}

				runtime.plan = &metadataPlan{params: func(context.Context) ([]string, error) { return nil, nil }}

				if _, err := service.Compile(lifetime, &wirev1.CompileRequest{ConnectionId: id, Source: &wirev1.Source{}}); err != nil {
					t.Fatalf("metadata failure retained plan quota: %v", err)
				}

				if err := connections.CloseConnection(lifetime, connection.ID()); err != nil {
					t.Fatal(err)
				}

				if hosted.closed != 1 {
					t.Fatal("failed metadata plan was registered and closed again")
				}
			})
		}
	}
}
