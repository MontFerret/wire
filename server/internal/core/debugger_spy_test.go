package core

import (
	"context"
	"sort"
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
	nextID           debugger.BreakpointID
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

func (d *spyDebugger) Pause(_ context.Context) error {
	d.mu.Lock()
	d.pauseCalls++
	pause := d.pause
	d.mu.Unlock()

	if pause == nil {
		return nil
	}

	return pause()
}

func (d *spyDebugger) SetBreakpoint(ctx context.Context, position source.Location) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(ctx, position, debugger.BreakpointOptions{})
}

func (d *spyDebugger) SetBreakpointAt(_ context.Context, position source.Location, options debugger.BreakpointOptions) (debugger.Breakpoint, error) {
	d.mu.Lock()
	d.setCalls++
	setBreakpoint := d.setBreakpoint
	d.mu.Unlock()

	if setBreakpoint != nil {
		value, err := setBreakpoint(position, options)
		if err == nil {
			d.mu.Lock()
			if d.breakpoints == nil {
				d.breakpoints = make(map[debugger.BreakpointID]debugger.Breakpoint)
			}

			d.breakpoints[value.ID] = value
			d.mu.Unlock()
		}

		return value, err
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

func (d *spyDebugger) DeleteBreakpoint(_ context.Context, id debugger.BreakpointID) error {
	d.mu.Lock()
	d.deleteCalls++
	deleteBreakpoint := d.deleteBreakpoint
	d.mu.Unlock()

	if deleteBreakpoint != nil {
		err := deleteBreakpoint(id)
		if err == nil {
			d.mu.Lock()
			delete(d.breakpoints, id)
			d.mu.Unlock()
		}

		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.breakpoints, id)

	return nil
}

func (d *spyDebugger) Breakpoints(_ context.Context) ([]debugger.Breakpoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]debugger.Breakpoint, 0, len(d.breakpoints))
	for _, value := range d.breakpoints {
		result = append(result, value)
	}

	return result, nil
}

func (d *spyDebugger) Frames(_ context.Context) ([]debugger.Frame, error) {
	return append([]debugger.Frame(nil), d.frames...), nil
}

func (d *spyDebugger) Locals(_ context.Context) ([]debugger.Variable, error) {
	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) FrameLocals(_ context.Context, _ int) ([]debugger.Variable, error) {
	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) Variables(_ context.Context, reference debugger.ValueReference) ([]debugger.Variable, error) {
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

// ReplaceBreakpoints implements atomic fixture publication with stable IDs.
func (d *spyDebugger) ReplaceBreakpoints(ctx context.Context, sourceName string, requests []debugger.BreakpointRequest) ([]debugger.Breakpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.breakpoints == nil {
		d.breakpoints = make(map[debugger.BreakpointID]debugger.Breakpoint)
	}

	existing := make([]debugger.Breakpoint, 0, len(d.breakpoints))
	for _, value := range d.breakpoints {
		if value.ID > d.nextID {
			d.nextID = value.ID
		}

		if value.RequestedLocation.SourceName == sourceName {
			existing = append(existing, value)
		}
	}

	sort.Slice(existing, func(i, j int) bool { return existing[i].ID < existing[j].ID })
	result := make([]debugger.Breakpoint, len(requests))
	used := make(map[debugger.BreakpointID]bool)
	for i, request := range requests {
		location := source.Location{SourceName: sourceName, Position: request.Position}
		for _, value := range existing {
			if !used[value.ID] && value.RequestedLocation == location && value.BindingMode == request.Options.BindingMode {
				result[i] = value
				used[value.ID] = true

				break
			}
		}

		if result[i].ID == 0 {
			d.nextID++

			result[i] = debugger.Breakpoint{ID: d.nextID, RequestedLocation: location, Location: source.Range{Location: location}, BindingMode: request.Options.BindingMode, FunctionID: debugger.NoFunction, Bound: request.Position.Line < 100}
			if !result[i].Bound {
				result[i].Location = source.Range{}
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for _, value := range existing {
		delete(d.breakpoints, value.ID)
	}

	for _, value := range result {
		d.breakpoints[value.ID] = value
	}

	return result, nil
}
