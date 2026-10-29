package ferret_test

import (
	"bytes"
	"testing"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/wire/server"
)

func TestLargeNativeNormalAndDebuggerOutput(t *testing.T) {
	limits := server.DefaultLimits()
	limits.MaxOutboundMessageBytes = 1024
	h := newHarness(t, server.WithLimits(limits))
	src := api.Source{Content: `RETURN REPEAT("abcdefgh", 700000)`}
	want := append([]byte{'"'}, bytes.Repeat([]byte("abcdefgh"), 700000)...)
	want = append(want, '"')

	output, err := h.runtime.Run(h.ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	h.own(output)

	content, err := output.Collect(h.ctx)
	if err != nil || content == nil || !bytes.Equal(content.Data, want) {
		t.Fatalf("normal output mismatch: %v", err)
	}

	if output.Metadata() != content.Metadata {
		t.Fatal("native metadata changed during consumption")
	}

	plan, err := h.runtime.CompileDebug(h.ctx, src)
	if err != nil {
		t.Fatal(err)
	}

	h.own(plan)

	session, err := plan.NewDebugSession(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	h.own(session)

	event, err := session.Start(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	for event != nil && event.Reason != debugger.ReasonCompleted {
		event, err = session.Continue(h.ctx)
		if err != nil {
			t.Fatal(err)
		}
	}

	if event == nil || event.Output == nil || !bytes.Equal(event.Output.Data, want) {
		t.Fatal("debugger output mismatch")
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(event.Output.Data, want) {
		t.Fatal("debugger content expired with session")
	}
}
