package harness

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type (
	// DebuggerBehavior configures command, inspection, evaluation, and cleanup hooks.
	DebuggerBehavior struct {
		Command       func(context.Context, string, int) (*debugger.Event, error)
		Pause         func() error
		Inspect       func(string) error
		Evaluate      func(context.Context, int, string) (debugger.Value, error)
		Close         func() error
		Enumeration   func(context.Context) error
		BeforeReplace func(context.Context) error
		AfterReplace  func()
		Observe       func(context.Context, string) error
		Frames        []debugger.Frame
	}

	// BreakpointRequest records both location and binding options for transport assertions.
	BreakpointRequest struct {
		Location source.Location
		Options  debugger.BreakpointOptions
	}

	// DebuggerSpy records hosted debugger operations and maintains deterministic breakpoint fixtures.
	DebuggerSpy struct {
		id          int
		sourceName  string
		recorder    *Recorder
		behavior    DebuggerBehavior
		mu          sync.Mutex
		breakpoints map[debugger.BreakpointID]debugger.Breakpoint
		nextID      debugger.BreakpointID
	}
)

var _ debugger.Session = (*DebuggerSpy)(nil)

func newDebuggerSpy(recorder *Recorder, parent int, behavior DebuggerBehavior) *DebuggerSpy {
	return &DebuggerSpy{id: recorder.create("debugger", parent), recorder: recorder, behavior: behavior, breakpoints: make(map[debugger.BreakpointID]debugger.Breakpoint)}
}

func (d *DebuggerSpy) command(ctx context.Context, method string) (*debugger.Event, error) {
	call := d.recorder.record(Call{Resource: d.id, Method: method})
	defer d.recorder.record(Call{Resource: d.id, Method: method + "Finished"})

	if d.behavior.Command != nil {
		return d.behavior.Command(ctx, method, call)
	}

	reason := debugger.ReasonStep

	switch method {
	case "Start":
		reason = debugger.ReasonEntry
	case "Continue":
		reason = debugger.ReasonCompleted
	}

	return &debugger.Event{Reason: reason}, nil
}

// Start records an initial command, defaulting to an entry stop when no hook is set.
func (d *DebuggerSpy) Start(ctx context.Context) (*debugger.Event, error) {
	return d.command(ctx, "Start")
}

// Continue records a resume command, defaulting to completion when no hook is set.
func (d *DebuggerSpy) Continue(ctx context.Context) (*debugger.Event, error) {
	return d.command(ctx, "Continue")
}

// StepOver records a step-over command, defaulting to a step stop when no hook is set.
func (d *DebuggerSpy) StepOver(ctx context.Context) (*debugger.Event, error) {
	return d.command(ctx, "StepOver")
}

// StepIn records a step-in command, defaulting to a step stop when no hook is set.
func (d *DebuggerSpy) StepIn(ctx context.Context) (*debugger.Event, error) {
	return d.command(ctx, "StepIn")
}

// StepOut records a step-out command, defaulting to a step stop when no hook is set.
func (d *DebuggerSpy) StepOut(ctx context.Context) (*debugger.Event, error) {
	return d.command(ctx, "StepOut")
}

// Pause records the request before invoking the optional pause hook.
func (d *DebuggerSpy) Pause(ctx context.Context) error {
	if d.behavior.Observe != nil {
		if err := d.behavior.Observe(ctx, "Pause"); err != nil {
			return err
		}
	}

	d.recorder.record(Call{Resource: d.id, Method: "Pause"})

	if d.behavior.Pause != nil {
		return d.behavior.Pause()
	}

	return nil
}

// SetBreakpoint records a breakpoint request with default options.
func (d *DebuggerSpy) SetBreakpoint(ctx context.Context, location source.Location) (debugger.Breakpoint, error) {
	d.recorder.record(Call{Resource: d.id, Method: "SetBreakpoint"})

	return d.SetBreakpointAt(ctx, location, debugger.BreakpointOptions{})
}

// SetBreakpointAt records location and options and assigns deterministic binding metadata.
func (d *DebuggerSpy) SetBreakpointAt(ctx context.Context, location source.Location, options debugger.BreakpointOptions) (debugger.Breakpoint, error) {
	if d.behavior.Observe != nil {
		if err := d.behavior.Observe(ctx, "SetBreakpointAt"); err != nil {
			return debugger.Breakpoint{}, err
		}
	}

	d.recorder.record(Call{Resource: d.id, Method: "SetBreakpointAt", Argument: BreakpointRequest{Location: location, Options: options}})
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextID++
	value := debugger.Breakpoint{ID: d.nextID, RequestedLocation: location, Location: source.Range{Location: location, Span: source.Span{Start: 3, End: 8}}, PointID: debugger.PointID(10 + d.nextID), FunctionID: 7, BindingMode: options.BindingMode, Bound: true}
	d.breakpoints[value.ID] = value

	return value, nil
}

// DeleteBreakpoint records the ID and removes it from the fixture's breakpoint set.
func (d *DebuggerSpy) DeleteBreakpoint(ctx context.Context, id debugger.BreakpointID) error {
	if d.behavior.Observe != nil {
		if err := d.behavior.Observe(ctx, "DeleteBreakpoint"); err != nil {
			return err
		}
	}

	d.recorder.record(Call{Resource: d.id, Method: "DeleteBreakpoint", Argument: id})
	d.mu.Lock()
	delete(d.breakpoints, id)
	d.mu.Unlock()

	return nil
}

// Breakpoints returns the fixture's bindings sorted by ID for deterministic assertions.
func (d *DebuggerSpy) Breakpoints(ctx context.Context) ([]debugger.Breakpoint, error) {
	d.recorder.record(Call{Resource: d.id, Method: "Breakpoints"})

	if d.behavior.Enumeration != nil {
		if err := d.behavior.Enumeration(ctx); err != nil {
			return nil, err
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]debugger.Breakpoint, 0, len(d.breakpoints))

	for _, value := range d.breakpoints {
		result = append(result, value)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// Frames records inspection and returns two distinguishable frames after the inspection hook.
func (d *DebuggerSpy) Frames(ctx context.Context) ([]debugger.Frame, error) {
	if d.behavior.Observe != nil {
		if err := d.behavior.Observe(ctx, "Frames"); err != nil {
			return nil, err
		}
	}

	d.recorder.record(Call{Resource: d.id, Method: "Frames"})

	if d.behavior.Inspect != nil {
		if err := d.behavior.Inspect("Frames"); err != nil {
			return nil, err
		}
	}

	if d.behavior.Frames != nil {
		return d.behavior.Frames, nil
	}

	return []debugger.Frame{
		{Name: "top", Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 2, Column: 3}}, FunctionID: 7},
		{Name: "caller", Location: source.Location{SourceName: "caller.fql", Position: source.Position{Line: 4, Column: 5}}, FunctionID: 6},
	}, nil
}

// Locals uses frame zero so tests can distinguish default-frame access.
func (d *DebuggerSpy) Locals(ctx context.Context) ([]debugger.Variable, error) {
	d.recorder.record(Call{Resource: d.id, Method: "Locals"})

	return d.FrameLocals(ctx, 0)
}

// FrameLocals records the frame index and returns a distinguishable variable fixture.
func (d *DebuggerSpy) FrameLocals(ctx context.Context, frame int) ([]debugger.Variable, error) {
	if d.behavior.Observe != nil {
		if err := d.behavior.Observe(ctx, "FrameLocals"); err != nil {
			return nil, err
		}
	}

	d.recorder.record(Call{Resource: d.id, Method: "FrameLocals", Index: frame})

	return []debugger.Variable{{Name: fmt.Sprintf("local-%d", frame), Value: debugger.Value{Type: "object", Display: "{...}", Reference: 9}, Mutable: true, Param: frame == 1}}, nil
}

// Variables expands the fixture reference and rejects unknown references.
func (d *DebuggerSpy) Variables(ctx context.Context, reference debugger.ValueReference) ([]debugger.Variable, error) {
	if d.behavior.Observe != nil {
		if err := d.behavior.Observe(ctx, "Variables"); err != nil {
			return nil, err
		}
	}

	d.recorder.record(Call{Resource: d.id, Method: "Variables", Argument: reference})

	if reference != 9 {
		return nil, fmt.Errorf("unknown fixture reference %d", reference)
	}

	return []debugger.Variable{{Name: "child", Value: debugger.Value{Type: "string", Display: "value"}}}, nil
}

// Evaluate uses frame zero so tests can distinguish default-frame evaluation.
func (d *DebuggerSpy) Evaluate(ctx context.Context, expression string) (debugger.Value, error) {
	d.recorder.record(Call{Resource: d.id, Method: "Evaluate"})

	return d.EvaluateFrame(ctx, 0, expression)
}

// EvaluateFrame records frame and expression around the hook or deterministic fallback value.
func (d *DebuggerSpy) EvaluateFrame(ctx context.Context, frame int, expression string) (debugger.Value, error) {
	if d.behavior.Observe != nil {
		if err := d.behavior.Observe(ctx, "EvaluateFrame"); err != nil {
			return debugger.Value{}, err
		}
	}

	d.recorder.record(Call{Resource: d.id, Method: "EvaluateFrame", Index: frame, Argument: expression})
	defer d.recorder.record(Call{Resource: d.id, Method: "EvaluateFrameFinished"})

	if d.behavior.Evaluate != nil {
		return d.behavior.Evaluate(ctx, frame, expression)
	}

	return debugger.Value{Type: "string", Display: fmt.Sprintf("frame-%d:%s", frame, expression)}, nil
}

// Close records entry and settlement around the configured cleanup hook.
func (d *DebuggerSpy) Close() error {
	d.recorder.record(Call{Resource: d.id, Method: "Close"})
	defer d.recorder.record(Call{Resource: d.id, Method: "CloseFinished"})

	if d.behavior.Close != nil {
		return d.behavior.Close()
	}

	return nil
}

// ReplaceBreakpoints implements atomic fixture publication with stable IDs.
func (d *DebuggerSpy) ReplaceBreakpoints(ctx context.Context, sourceName string, requests []debugger.BreakpointRequest) ([]debugger.Breakpoint, error) {
	d.recorder.record(Call{Resource: d.id, Method: "ReplaceBreakpoints"})

	if sourceName == "" {
		sourceName = d.sourceName
	}

	if d.behavior.BeforeReplace != nil {
		if err := d.behavior.BeforeReplace(ctx); err != nil {
			return nil, err
		}
	}

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

	if d.behavior.AfterReplace != nil {
		d.behavior.AfterReplace()
	}

	return result, nil
}
