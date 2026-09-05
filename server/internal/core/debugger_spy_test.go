package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type spyDebugger struct {
	mu               sync.Mutex
	start            func(context.Context) (*debugger.Event, error)
	resume           func(context.Context) (*debugger.Event, error)
	pause            func() error
	setBreakpoint    func(source.Location, debugger.BreakpointOptions) (debugger.Breakpoint, error)
	deleteBreakpoint func(debugger.BreakpointID) error
	breakpoints      map[debugger.BreakpointID]debugger.Breakpoint
	frames           []debugger.Frame
	locals           []debugger.Variable
	variables        func(debugger.ValueReference) ([]debugger.Variable, error)
	setCalls         int
	deleteCalls      int
	pauseCalls       int
	close            func() error
	closeCalls       int
}

func (d *spyDebugger) Start(ctx context.Context) (*debugger.Event, error) {
	if d.start == nil {
		return &debugger.Event{Reason: debugger.ReasonEntry}, nil
	}

	return d.start(ctx)
}

func (d *spyDebugger) Continue(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) StepIn(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) StepOver(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) StepOut(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) resumeDebug(ctx context.Context) (*debugger.Event, error) {
	if d.resume == nil {
		return &debugger.Event{Reason: debugger.ReasonCompleted}, nil
	}

	return d.resume(ctx)
}

func (d *spyDebugger) Pause() error {
	d.mu.Lock()
	d.pauseCalls++
	pause := d.pause
	d.mu.Unlock()

	if pause == nil {
		return nil
	}

	return pause()
}

func (d *spyDebugger) SetBreakpoint(position source.Location) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(position, debugger.BreakpointOptions{})
}

func (d *spyDebugger) SetBreakpointAt(position source.Location, options debugger.BreakpointOptions) (debugger.Breakpoint, error) {
	d.mu.Lock()
	d.setCalls++
	setBreakpoint := d.setBreakpoint
	d.mu.Unlock()

	if setBreakpoint != nil {
		return setBreakpoint(position, options)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.breakpoints == nil {
		d.breakpoints = make(map[debugger.BreakpointID]debugger.Breakpoint)
	}

	id := debugger.BreakpointID(len(d.breakpoints) + 1)
	value := debugger.Breakpoint{
		Location: source.Range{
			Location: position,
			Span:     source.Span{Start: 0, End: 1},
		},
		RequestedLocation: position,
		ID:                id,
		PointID:           41,
		FunctionID:        42,
		BindingMode:       options.BindingMode,
		Bound:             true,
	}
	d.breakpoints[id] = value

	return value, nil
}

func (d *spyDebugger) DeleteBreakpoint(id debugger.BreakpointID) error {
	d.mu.Lock()
	d.deleteCalls++
	deleteBreakpoint := d.deleteBreakpoint
	d.mu.Unlock()

	if deleteBreakpoint != nil {
		return deleteBreakpoint(id)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.breakpoints, id)

	return nil
}

func (d *spyDebugger) Breakpoints() []debugger.Breakpoint {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]debugger.Breakpoint, 0, len(d.breakpoints))
	for _, value := range d.breakpoints {
		result = append(result, value)
	}

	return result
}

func (d *spyDebugger) Frames() ([]debugger.Frame, error) {
	return append([]debugger.Frame(nil), d.frames...), nil
}

func (d *spyDebugger) Locals() ([]debugger.Variable, error) {
	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) FrameLocals(int) ([]debugger.Variable, error) {
	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	if d.variables != nil {
		return d.variables(reference)
	}

	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) Evaluate(context.Context, string) (debugger.Value, error) {
	return debugger.Value{Type: "string", Display: "wire"}, nil
}

func (d *spyDebugger) EvaluateFrame(context.Context, int, string) (debugger.Value, error) {
	return debugger.Value{Type: "string", Display: "wire"}, nil
}

func (d *spyDebugger) Close() error {
	d.mu.Lock()
	d.closeCalls++
	closeDebugger := d.close
	d.mu.Unlock()

	if closeDebugger == nil {
		return nil
	}

	return closeDebugger()
}

func (d *spyDebugger) closes() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.closeCalls
}

func (d *spyDebugger) breakpointCalls() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.setCalls, d.deleteCalls
}

func (d *spyDebugger) pauses() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.pauseCalls
}
