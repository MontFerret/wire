package server_test

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type contractDebugger struct {
	mu               sync.Mutex
	continueStarted  chan struct{}
	pauseRequested   chan struct{}
	breakpoints      map[debugger.BreakpointID]debugger.Breakpoint
	nextBreakpointID debugger.BreakpointID
	commands         []string
	frameLocals      []int
	evaluateFrames   []int
	pauseOnce        sync.Once
	continueCalls    int
	closeCalls       int
}

func newContractDebugger() *contractDebugger {
	return &contractDebugger{
		continueStarted: make(chan struct{}),
		pauseRequested:  make(chan struct{}),
		breakpoints:     make(map[debugger.BreakpointID]debugger.Breakpoint),
	}
}

func (d *contractDebugger) Start(context.Context) (*debugger.Event, error) {
	d.recordCommand("start")

	return &debugger.Event{
		Reason: debugger.ReasonEntry,
		Location: source.Range{Location: source.Location{
			SourceName: "debug.fql",
			Position:   source.Position{Line: 1, Column: 1},
		}},
		Depth: 2,
	}, nil
}

func (d *contractDebugger) Continue(context.Context) (*debugger.Event, error) {
	d.recordCommand("continue")
	d.mu.Lock()
	d.continueCalls++
	call := d.continueCalls
	d.mu.Unlock()
	if call == 1 {
		close(d.continueStarted)
		<-d.pauseRequested

		return &debugger.Event{Reason: debugger.ReasonPause}, nil
	}

	return &debugger.Event{
		Reason: debugger.ReasonCompleted,
		Output: &api.Output{ContentType: "application/json", Content: []byte(`{"done":true}`)},
	}, nil
}

func (d *contractDebugger) StepIn(context.Context) (*debugger.Event, error) {
	d.recordCommand("step-in")

	return &debugger.Event{
		Reason:           debugger.ReasonBreakpoint,
		HitBreakpointIDs: []debugger.BreakpointID{2},
		Location: source.Range{
			Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 1, Column: 3}},
			Span:     source.Span{Start: 3, End: 4},
		},
		Depth: 3,
	}, nil
}

func (d *contractDebugger) StepOver(context.Context) (*debugger.Event, error) {
	d.recordCommand("step-over")

	return &debugger.Event{Reason: debugger.ReasonStep}, nil
}

func (d *contractDebugger) StepOut(context.Context) (*debugger.Event, error) {
	d.recordCommand("step-out")

	return &debugger.Event{Reason: debugger.ReasonRuntimeError, Error: errors.New("runtime error")}, nil
}

func (d *contractDebugger) Pause() error {
	d.pauseOnce.Do(func() { close(d.pauseRequested) })

	return nil
}

func (d *contractDebugger) SetBreakpoint(location source.Location) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(location, debugger.BreakpointOptions{})
}

func (d *contractDebugger) SetBreakpointAt(
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nextBreakpointID++
	value := debugger.Breakpoint{
		ID:                d.nextBreakpointID,
		RequestedLocation: location,
		Location:          source.Range{Location: location, Span: source.Span{Start: 0, End: 1}},
		PointID:           debugger.PointID(10 + d.nextBreakpointID),
		FunctionID:        7,
		BindingMode:       options.BindingMode,
		Bound:             true,
	}
	d.breakpoints[value.ID] = value

	return value, nil
}

func (d *contractDebugger) DeleteBreakpoint(id debugger.BreakpointID) error {
	d.mu.Lock()
	delete(d.breakpoints, id)
	d.mu.Unlock()

	return nil
}

func (d *contractDebugger) Breakpoints() []debugger.Breakpoint {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]debugger.Breakpoint, 0, len(d.breakpoints))
	for _, value := range d.breakpoints {
		result = append(result, value)
	}

	return result
}

func (d *contractDebugger) Frames() ([]debugger.Frame, error) {
	return []debugger.Frame{
		{Name: "top", Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 2}}, FunctionID: 7},
		{Name: "caller", Location: source.Location{SourceName: "debug.fql", Position: source.Position{Line: 1}}, FunctionID: 6},
	}, nil
}

func (d *contractDebugger) Locals() ([]debugger.Variable, error) {
	return d.FrameLocals(0)
}

func (d *contractDebugger) FrameLocals(frame int) ([]debugger.Variable, error) {
	d.mu.Lock()
	d.frameLocals = append(d.frameLocals, frame)
	d.mu.Unlock()

	return []debugger.Variable{{
		Name:    "local",
		Value:   debugger.Value{Type: "object", Display: "{...}", Reference: 9},
		Mutable: true,
	}}, nil
}

func (d *contractDebugger) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	if reference != 9 {
		return nil, errors.New("unexpected reference")
	}

	return []debugger.Variable{{Name: "child", Value: debugger.Value{Type: "string", Display: "value"}}}, nil
}

func (d *contractDebugger) Evaluate(ctx context.Context, expression string) (debugger.Value, error) {
	return d.EvaluateFrame(ctx, 0, expression)
}

func (d *contractDebugger) EvaluateFrame(
	_ context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
	d.mu.Lock()
	d.evaluateFrames = append(d.evaluateFrames, frame)
	d.mu.Unlock()

	return debugger.Value{Type: "string", Display: "frame-" + string(rune('0'+frame)) + ":" + expression}, nil
}

func (d *contractDebugger) Close() error {
	d.mu.Lock()
	d.closeCalls++
	d.mu.Unlock()

	return nil
}

func (d *contractDebugger) recordCommand(command string) {
	d.mu.Lock()
	d.commands = append(d.commands, command)
	d.mu.Unlock()
}
