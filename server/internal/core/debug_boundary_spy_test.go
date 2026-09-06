package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type (
	boundaryDebugger struct {
		spyDebugger
		callsMu sync.Mutex
		calls   []string
		panicOn string
		err     error
	}

	borrowedInspectionDebugger struct {
		spyDebugger
		frames []debugger.Frame
		locals []debugger.Variable
		values []debugger.Variable
	}
)

func (d *boundaryDebugger) Start(context.Context) (*debugger.Event, error) {
	return d.command("start")
}

func (d *boundaryDebugger) Continue(context.Context) (*debugger.Event, error) {
	return d.command("continue")
}

func (d *boundaryDebugger) StepOver(context.Context) (*debugger.Event, error) {
	return d.command("step-over")
}

func (d *boundaryDebugger) StepIn(context.Context) (*debugger.Event, error) {
	return d.command("step-in")
}

func (d *boundaryDebugger) StepOut(context.Context) (*debugger.Event, error) {
	return d.command("step-out")
}

func (d *boundaryDebugger) Pause() error {
	return d.record("pause")
}

func (d *boundaryDebugger) Frames() ([]debugger.Frame, error) {
	if err := d.record("frames"); err != nil {
		return nil, err
	}

	return []debugger.Frame{{Name: "main"}}, nil
}

func (d *boundaryDebugger) FrameLocals(frame int) ([]debugger.Variable, error) {
	if err := d.record("frame-locals"); err != nil {
		return nil, err
	}

	return []debugger.Variable{{Name: "frame"}}, nil
}

func (d *boundaryDebugger) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	if err := d.record("variables"); err != nil {
		return nil, err
	}

	return []debugger.Variable{{Name: "reference"}}, nil
}

func (d *boundaryDebugger) EvaluateFrame(
	context.Context,
	int,
	string,
) (debugger.Value, error) {
	if err := d.record("evaluate-frame"); err != nil {
		return debugger.Value{}, err
	}

	return debugger.Value{Type: "string", Display: "value"}, nil
}

func (d *boundaryDebugger) SetBreakpointAt(
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	if err := d.record("set-breakpoint"); err != nil {
		return debugger.Breakpoint{}, err
	}

	return debugger.Breakpoint{ID: 7, RequestedLocation: location, BindingMode: options.BindingMode}, nil
}

func (d *boundaryDebugger) DeleteBreakpoint(debugger.BreakpointID) error {
	return d.record("delete-breakpoint")
}

func (d *boundaryDebugger) Close() error {
	return d.record("close")
}

func (d *boundaryDebugger) command(name string) (*debugger.Event, error) {
	if err := d.record(name); err != nil {
		return nil, err
	}

	return &debugger.Event{Reason: debugger.ReasonStep}, nil
}

func (d *boundaryDebugger) record(name string) error {
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

func (d *boundaryDebugger) snapshotCalls() []string {
	d.callsMu.Lock()
	defer d.callsMu.Unlock()

	return append([]string(nil), d.calls...)
}

func (d *borrowedInspectionDebugger) Frames() ([]debugger.Frame, error) {
	return d.frames, nil
}

func (d *borrowedInspectionDebugger) FrameLocals(int) ([]debugger.Variable, error) {
	return d.locals, nil
}

func (d *borrowedInspectionDebugger) Variables(debugger.ValueReference) ([]debugger.Variable, error) {
	return d.values, nil
}
