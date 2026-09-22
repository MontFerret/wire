package core

import (
	"context"
	"testing"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

func BenchmarkReplaceBreakpoints(b *testing.B) {
	ctx := context.Background()
	limits := testLimits().resources()
	limits.Breakpoints = 256
	plan := &Plan{store: newResourceStore(ctx, limits)}
	session := newDebugSession(plan, &spyDebugger{})
	requests := make([]debugger.BreakpointRequest, 32)
	for i := range requests {
		requests[i].Position = source.Position{Line: i + 1}
	}

	b.Cleanup(func() {
		if err := session.Close(ctx); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := session.ReplaceBreakpoints(ctx, "query.fql", requests); err != nil {
			b.Fatal(err)
		}
	}
}
