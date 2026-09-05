package server_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestNewRuntimePreservesUnavailableStatus(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient(
		"passthrough:///unavailable-wire-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	_, err = client.NewRuntime(testContext(t), connection)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("NewRuntime changed unavailable transport status: %v", err)
	}
}

func TestTransportLossCleansRuntimeAdapterDescendants(t *testing.T) {
	started := make(chan struct{})
	settled := make(chan struct{})
	var runOnce sync.Once
	hostedSession := &contractSession{run: func(ctx context.Context, _ int) (api.Output, error) {
		runOnce.Do(func() { close(started) })
		<-ctx.Done()
		close(settled)

		return api.Output{}, ctx.Err()
	}}
	hostedPlan := &contractPlan{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
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
	session, err := plan.NewSession(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	runResult := make(chan error, 1)
	go func() {
		_, runErr := session.Run(context.Background())
		runResult <- runErr
	}()
	<-started

	if err := env.conn.Close(); err != nil {
		t.Fatal(err)
	}
	env.shutdown = true
	env.transportClosed = true
	if err := <-runResult; status.Code(err) != codes.Unavailable && status.Code(err) != codes.Canceled {
		t.Fatalf("transport loss status was not preserved: %v", err)
	}
	select {
	case <-settled:
	case <-time.After(5 * time.Second):
		t.Fatal("transport loss did not cancel hosted Session.Run")
	}

	deadline := time.Now().Add(5 * time.Second)
	cleanupSettled := false
	for time.Now().Before(deadline) {
		_, sessionCloseCalls := hostedSession.counts()
		hostedPlan.mu.Lock()
		planCloseCalls := hostedPlan.closeCalls
		hostedPlan.mu.Unlock()
		if sessionCloseCalls == 1 && planCloseCalls == 1 {
			cleanupSettled = true

			break
		}
		time.Sleep(time.Millisecond)
	}

	if !cleanupSettled {
		_, sessionCloseCalls := hostedSession.counts()
		hostedPlan.mu.Lock()
		planCloseCalls := hostedPlan.closeCalls
		hostedPlan.mu.Unlock()
		t.Fatalf("transport loss cleanup did not settle: session=%d plan=%d", sessionCloseCalls, planCloseCalls)
	}
	hosted.mu.Lock()
	runtimeCloseCalls := hosted.closeCalls
	hosted.mu.Unlock()
	if runtimeCloseCalls != 0 {
		t.Fatalf("transport loss closed borrowed Runtime %d times", runtimeCloseCalls)
	}
}
