package core

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/internal/panicboundary"
)

type controllerDebugger struct {
	spyDebugger
	callsMu sync.Mutex
	calls   []string
	panicOn string
	err     error
}

func (d *controllerDebugger) Start(context.Context) (*debugger.Event, error) {
	return d.command("start")
}

func (d *controllerDebugger) Continue(context.Context) (*debugger.Event, error) {
	return d.command("continue")
}

func (d *controllerDebugger) StepOver(context.Context) (*debugger.Event, error) {
	return d.command("step-over")
}

func (d *controllerDebugger) StepIn(context.Context) (*debugger.Event, error) {
	return d.command("step-in")
}

func (d *controllerDebugger) StepOut(context.Context) (*debugger.Event, error) {
	return d.command("step-out")
}

func (d *controllerDebugger) Pause() error {
	return d.record("pause")
}

func (d *controllerDebugger) Frames() ([]debugger.Frame, error) {
	if err := d.record("frames"); err != nil {
		return nil, err
	}

	return []debugger.Frame{{Name: "main"}}, nil
}

func (d *controllerDebugger) FrameLocals(frame int) ([]debugger.Variable, error) {
	if err := d.record("frame-locals"); err != nil {
		return nil, err
	}

	return []debugger.Variable{{Name: "frame"}}, nil
}

func (d *controllerDebugger) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	if err := d.record("variables"); err != nil {
		return nil, err
	}

	return []debugger.Variable{{Name: "reference"}}, nil
}

func (d *controllerDebugger) EvaluateFrame(
	context.Context,
	int,
	string,
) (debugger.Value, error) {
	if err := d.record("evaluate-frame"); err != nil {
		return debugger.Value{}, err
	}

	return debugger.Value{Type: "string", Display: "value"}, nil
}

func (d *controllerDebugger) SetBreakpointAt(
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	if err := d.record("set-breakpoint"); err != nil {
		return debugger.Breakpoint{}, err
	}

	return debugger.Breakpoint{ID: 7, RequestedLocation: location, BindingMode: options.BindingMode}, nil
}

func (d *controllerDebugger) DeleteBreakpoint(debugger.BreakpointID) error {
	return d.record("delete-breakpoint")
}

func (d *controllerDebugger) Close() error {
	return d.record("close")
}

func (d *controllerDebugger) command(name string) (*debugger.Event, error) {
	if err := d.record(name); err != nil {
		return nil, err
	}

	return &debugger.Event{Reason: debugger.ReasonStep}, nil
}

func (d *controllerDebugger) record(name string) error {
	d.callsMu.Lock()
	d.calls = append(d.calls, name)
	panicOn := d.panicOn
	err := d.err
	d.callsMu.Unlock()

	if panicOn == name {
		panic("runtime secret")
	}

	return err
}

func (d *controllerDebugger) snapshotCalls() []string {
	d.callsMu.Lock()
	defer d.callsMu.Unlock()

	return append([]string(nil), d.calls...)
}

func TestDebugControllerDelegatesCommandsAndInspection(t *testing.T) {
	runtime := &controllerDebugger{}
	controller := newDebugController(runtime)
	ctx := context.Background()

	commands := []func(context.Context) (*debugger.Event, error){
		controller.Start,
		controller.Continue,
		controller.StepOver,
		controller.StepIn,
		controller.StepOut,
	}
	for _, command := range commands {
		event, err := command(ctx)
		if err != nil || event == nil || event.Reason != debugger.ReasonStep {
			t.Fatalf("unexpected command result: %#v, %v", event, err)
		}
	}

	if err := controller.Pause(); err != nil {
		t.Fatal(err)
	}

	frames, err := controller.Frames()
	if err != nil || !reflect.DeepEqual(frames, []debugger.Frame{{Name: "main"}}) {
		t.Fatalf("unexpected frames: %#v, %v", frames, err)
	}

	locals, err := controller.FrameLocals(3)
	if err != nil || !reflect.DeepEqual(locals, []debugger.Variable{{Name: "frame"}}) {
		t.Fatalf("unexpected frame locals: %#v, %v", locals, err)
	}

	variables, err := controller.Variables(9)
	if err != nil || !reflect.DeepEqual(variables, []debugger.Variable{{Name: "reference"}}) {
		t.Fatalf("unexpected variables: %#v, %v", variables, err)
	}

	value, err := controller.EvaluateFrame(ctx, 3, "value")
	if err != nil || value != (debugger.Value{Type: "string", Display: "value"}) {
		t.Fatalf("unexpected evaluated value: %#v, %v", value, err)
	}

	location := source.Location{SourceName: "query.fql", Position: source.Position{Line: 1}}
	options := debugger.BreakpointOptions{BindingMode: debugger.BreakpointBindExact}
	breakpoint, err := controller.SetBreakpoint(location, options)
	if err != nil || breakpoint.ID != 7 || breakpoint.RequestedLocation != location || breakpoint.BindingMode != options.BindingMode {
		t.Fatalf("unexpected breakpoint: %#v, %v", breakpoint, err)
	}

	if err := controller.DeleteBreakpoint(breakpoint.ID); err != nil {
		t.Fatal(err)
	}

	wantCalls := []string{
		"start",
		"continue",
		"step-over",
		"step-in",
		"step-out",
		"pause",
		"frames",
		"frame-locals",
		"variables",
		"evaluate-frame",
		"set-breakpoint",
		"delete-breakpoint",
	}
	if got := runtime.snapshotCalls(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("unexpected controller calls: %#v", got)
	}
}

func TestDebugControllerReturnsRuntimeErrors(t *testing.T) {
	runtimeErr := errors.New("runtime failed")
	runtime := &controllerDebugger{err: runtimeErr}
	controller := newDebugController(runtime)

	if _, err := controller.Start(context.Background()); !errors.Is(err, runtimeErr) {
		t.Fatalf("command error was not retained: %v", err)
	}

	if _, err := controller.Frames(); !errors.Is(err, runtimeErr) {
		t.Fatalf("inspection error was not retained: %v", err)
	}

	if _, err := controller.SetBreakpoint(source.Location{}, debugger.BreakpointOptions{}); !errors.Is(err, runtimeErr) {
		t.Fatalf("breakpoint error was not retained: %v", err)
	}
}

func TestDebugControllerContainsRuntimePanicsAtEachBoundary(t *testing.T) {
	tests := []struct {
		name string
		call func(*DebugController) error
	}{
		{name: "start", call: func(controller *DebugController) error {
			_, err := controller.Start(context.Background())

			return err
		}},
		{name: "continue", call: func(controller *DebugController) error {
			_, err := controller.Continue(context.Background())

			return err
		}},
		{name: "step-over", call: func(controller *DebugController) error {
			_, err := controller.StepOver(context.Background())

			return err
		}},
		{name: "step-in", call: func(controller *DebugController) error {
			_, err := controller.StepIn(context.Background())

			return err
		}},
		{name: "step-out", call: func(controller *DebugController) error {
			_, err := controller.StepOut(context.Background())

			return err
		}},
		{name: "pause", call: func(controller *DebugController) error {
			return controller.Pause()
		}},
		{name: "frames", call: func(controller *DebugController) error {
			_, err := controller.Frames()

			return err
		}},
		{name: "frame-locals", call: func(controller *DebugController) error {
			_, err := controller.FrameLocals(0)

			return err
		}},
		{name: "variables", call: func(controller *DebugController) error {
			_, err := controller.Variables(1)

			return err
		}},
		{name: "evaluate-frame", call: func(controller *DebugController) error {
			_, err := controller.EvaluateFrame(context.Background(), 0, "value")

			return err
		}},
		{name: "set-breakpoint", call: func(controller *DebugController) error {
			_, err := controller.SetBreakpoint(source.Location{}, debugger.BreakpointOptions{})

			return err
		}},
		{name: "delete-breakpoint", call: func(controller *DebugController) error {
			return controller.DeleteBreakpoint(1)
		}},
		{name: "close", call: func(controller *DebugController) error {
			return controller.Close()
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &controllerDebugger{panicOn: test.name}
			err := test.call(newDebugController(runtime))
			if err == nil || strings.Contains(err.Error(), "runtime secret") {
				t.Fatalf("runtime panic was not sanitized: %v", err)
			}

			var panicErr *panicboundary.Error
			if !errors.As(err, &panicErr) {
				t.Fatalf("runtime panic was not retained as a typed cause: %v", err)
			}

			if panicErr.Value != "runtime secret" || len(panicErr.Stack) == 0 {
				t.Fatalf("runtime panic diagnostics were not retained: %#v", panicErr)
			}
		})
	}
}

func TestDebugControllerCloseIsConcurrentAndIdempotent(t *testing.T) {
	closeErr := errors.New("close failed")
	runtime := &controllerDebugger{err: closeErr}
	controller := newDebugController(runtime)

	const callers = 16
	results := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			ready.Done()
			<-start
			results <- controller.Close()
		}()
	}

	ready.Wait()
	close(start)
	for range callers {
		select {
		case err := <-results:
			if !errors.Is(err, closeErr) {
				t.Fatalf("close result was not retained: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for concurrent close")
		}
	}

	if got := runtime.snapshotCalls(); !reflect.DeepEqual(got, []string{"close"}) {
		t.Fatalf("runtime close calls = %#v", got)
	}
}
