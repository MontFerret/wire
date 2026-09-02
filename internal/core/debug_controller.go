package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

// DebugController exclusively owns the Unified API debugger session and keeps
// panic recovery at that external boundary. Wire state and policy remain owned
// by DebugSession and its collaborators.
type DebugController struct {
	session   debugger.Session
	closeOnce sync.Once
	closeErr  error
}

func newDebugController(session debugger.Session) *DebugController {
	return &DebugController{session: session}
}

func (c *DebugController) Start(ctx context.Context) (*debugger.Event, error) {
	return c.event("runtime debug start panicked", func() (*debugger.Event, error) {
		return c.session.Start(ctx)
	})
}

func (c *DebugController) Continue(ctx context.Context) (*debugger.Event, error) {
	return c.event("runtime debug continue panicked", func() (*debugger.Event, error) {
		return c.session.Continue(ctx)
	})
}

func (c *DebugController) Next(ctx context.Context) (*debugger.Event, error) {
	return c.event("runtime debug next panicked", func() (*debugger.Event, error) {
		return c.session.Next(ctx)
	})
}

func (c *DebugController) Step(ctx context.Context) (*debugger.Event, error) {
	return c.event("runtime debug step panicked", func() (*debugger.Event, error) {
		return c.session.Step(ctx)
	})
}

func (c *DebugController) Out(ctx context.Context) (*debugger.Event, error) {
	return c.event("runtime debug out panicked", func() (*debugger.Event, error) {
		return c.session.Out(ctx)
	})
}

func (c *DebugController) Pause() error {
	return callAPIError("runtime debug pause panicked", c.session.Pause)
}

func (c *DebugController) Frames() ([]debugger.Frame, error) {
	values, err, _ := callAPI("runtime debug frames panicked", c.session.Frames)
	if err != nil {
		return nil, err
	}

	return append([]debugger.Frame(nil), values...), nil
}

func (c *DebugController) FrameLocals(frame int) ([]debugger.Variable, error) {
	values, err, _ := callAPI("runtime debug frame locals panicked", func() ([]debugger.Variable, error) {
		return c.session.FrameLocals(frame)
	})
	if err != nil {
		return nil, err
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (c *DebugController) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	values, err, _ := callAPI("runtime debug variables panicked", func() ([]debugger.Variable, error) {
		return c.session.Variables(reference)
	})
	if err != nil {
		return nil, err
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (c *DebugController) EvaluateFrame(
	ctx context.Context,
	frame int,
	expression string,
) (debugger.Value, error) {
	value, err, _ := callAPI("runtime debug frame evaluation panicked", func() (debugger.Value, error) {
		return c.session.EvaluateFrame(ctx, frame, expression)
	})

	return value, err
}

func (c *DebugController) SetBreakpoint(
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	value, err, _ := callAPI("runtime debug set breakpoint panicked", func() (debugger.Breakpoint, error) {
		return c.session.SetBreakpointAt(location, options)
	})

	return value, err
}

func (c *DebugController) DeleteBreakpoint(id debugger.BreakpointID) error {
	return callAPIError("runtime debug delete breakpoint panicked", func() error {
		return c.session.DeleteBreakpoint(id)
	})
}

func (c *DebugController) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = closeAPIResource(c.session, "runtime debug cleanup panicked")
	})

	return c.closeErr
}

func (c *DebugController) event(
	panicMessage string,
	command func() (*debugger.Event, error),
) (*debugger.Event, error) {
	event, err, _ := callAPI(panicMessage, command)

	return event, err
}
