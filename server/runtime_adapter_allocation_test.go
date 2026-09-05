package server_test

import (
	"context"
	"errors"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeAdapterReleasesAllocationsThatRaceCancellation(t *testing.T) {
	t.Run("plan", func(t *testing.T) {
		started := make(chan struct{})
		finish := make(chan struct{})
		hostedPlan := &contractPlan{}
		hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
			close(started)
			<-finish

			return hostedPlan, nil
		}}
		env := newIntegrationEnv(t, hosted)
		remote, err := client.NewRuntime(testContext(t), env.conn)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, compileErr := remote.Compile(ctx, api.Source{Content: "RETURN 1"})
			result <- compileErr
		}()
		<-started
		cancel()
		close(finish)
		if err := <-result; !cancellationError(err) {
			t.Fatalf("compile cancellation was not preserved: %v", err)
		}
		hostedPlan.mu.Lock()
		closeCalls := hostedPlan.closeCalls
		hostedPlan.mu.Unlock()
		if closeCalls != 1 {
			t.Fatalf("Plan published during cancellation closed %d times", closeCalls)
		}
		if err := remote.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("normal session", func(t *testing.T) {
		started := make(chan struct{})
		finish := make(chan struct{})
		hostedSession := &contractSession{}
		hostedPlan := &contractPlan{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
			close(started)
			<-finish

			return hostedSession, nil
		}}
		hosted := &contractRuntime{compile: func(context.Context, api.Source, bool, contractPlanOptions) (api.Plan, error) {
			return hostedPlan, nil
		}}
		env := newIntegrationEnv(t, hosted)
		remote, err := client.NewRuntime(testContext(t), env.conn)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := remote.Compile(testContext(t), api.Source{Content: "RETURN 1"})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, createErr := plan.NewSession(ctx)
			result <- createErr
		}()
		<-started
		cancel()
		close(finish)
		if err := <-result; !cancellationError(err) {
			t.Fatalf("session cancellation was not preserved: %v", err)
		}
		if runs, closes := hostedSession.counts(); runs != 0 || closes != 1 {
			t.Fatalf("Session published during cancellation leaked: runs=%d closes=%d", runs, closes)
		}
		if err := plan.Close(); err != nil {
			t.Fatal(err)
		}
		if err := remote.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func cancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}
