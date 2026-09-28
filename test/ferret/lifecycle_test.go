package ferret_test

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/client"
	"github.com/MontFerret/wire/server"
)

func TestPlanClosePreservesSession(t *testing.T) {
	h := newHarness(t)

	plan, err := h.runtime.CompileDebug(h.ctx, api.NewSource("child.fql", "RETURN @value"),
		api.WithOptimizationLevel(api.OptimizationNone))
	if err != nil {
		t.Fatal(err)
	}

	h.own(plan)

	session, err := plan.NewSession(h.ctx, api.WithParam("value", 42), api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	h.own(session)

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}

	if child, err := plan.NewSession(h.ctx); child != nil || !errors.Is(err, client.ErrClosed) {
		if child != nil {
			h.own(child)
		}

		t.Fatalf("closed plan NewSession = %T, %v", child, err)
	}

	if child, err := plan.NewDebugSession(h.ctx); child != nil || !errors.Is(err, client.ErrClosed) {
		if child != nil {
			h.own(child)
		}

		t.Fatalf("closed plan NewDebugSession = %T, %v", child, err)
	}

	requireDetachedParams(t, plan)

	output, err := session.Run(h.ctx)
	if err != nil {
		t.Fatalf("session lost its independent lifetime: %v", err)
	}

	requireJSONOutput(t, output, "42")
}

func TestRuntimeCloseDefersConnectionRelease(t *testing.T) {
	limits := server.DefaultLimits()
	limits.MaxConnections = 1
	h := newHarness(t, server.WithLimits(limits))
	src := api.NewSource("retained.fql", "RETURN 42")

	plan, err := h.runtime.Compile(h.ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	h.own(plan)

	session, err := plan.NewSession(h.ctx, api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	h.own(session)

	if err := h.runtime.Close(); err != nil {
		t.Fatal(err)
	}

	if output, err := h.runtime.Run(h.ctx, src); output != nil || !errors.Is(err, client.ErrClosed) {
		t.Fatalf("closed runtime Run = %+v, %v", output, err)
	}

	for _, compile := range []func() (api.Plan, error){
		func() (api.Plan, error) { return h.runtime.Compile(h.ctx, src) },
		func() (api.Plan, error) { return h.runtime.CompileDebug(h.ctx, src) },
	} {
		child, err := compile()
		if child != nil {
			h.own(child)
		}

		if child != nil || !errors.Is(err, client.ErrClosed) {
			t.Fatalf("closed runtime compile = %T, %v", child, err)
		}
	}

	extra, err := plan.NewSession(h.ctx, api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatalf("runtime closure gated surviving plan: %v", err)
	}

	h.own(extra)

	output, err := extra.Run(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	requireJSONOutput(t, output, "42")

	if err := extra.Close(); err != nil {
		t.Fatal(err)
	}

	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}

	output, err = session.Run(h.ctx)
	if err != nil {
		t.Fatalf("session lost its independent lifetime: %v", err)
	}

	requireJSONOutput(t, output, "42")

	// A public quota proves the connection is still charged while the final
	// child survives, without inspecting private IDs or server resource stores.
	other, err := h.openRuntime()
	if other != nil || status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("retained connection did not occupy its slot: runtime=%T err=%v", other, err)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	// Close waits for reclamation. No polling or sleep is needed to reuse the
	// slot, and reusing this transport also verifies that Wire only borrowed it.
	other, err = h.openRuntime()
	if err != nil {
		t.Fatalf("final descendant did not release connection: %v", err)
	}

	output, err = other.Run(h.ctx, src, api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatal(err)
	}

	requireJSONOutput(t, output, "42")
}

func TestWireShutdownBorrowsNativeRuntime(t *testing.T) {
	h := newHarness(t)
	src := api.NewSource("ownership.fql", "RETURN 42")

	plan, err := h.runtime.Compile(h.ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	h.own(plan)

	session, err := plan.NewSession(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	h.own(session)

	output, err := session.Run(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	requireJSONOutput(t, output, "42")

	if err := h.closeWire(); err != nil {
		t.Fatal(err)
	}

	output, err = h.hosted.Run(h.ctx, src, api.WithOutputContentType("application/json"))
	if err != nil {
		t.Fatalf("Wire shutdown closed the borrowed native runtime: %v", err)
	}

	requireJSONOutput(t, output, "42")

	if err := h.native.Close(); err != nil {
		t.Fatal(err)
	}
}
