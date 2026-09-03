package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/internal/panicboundary"
)

// DebugController exclusively owns the Unified API debugger session and keeps
// panic containment at that external boundary. Wire state and policy remain
// owned by DebugSession and its collaborators.
type DebugController struct {
	session   debugger.Session
	closeOnce sync.Once
	closeErr  error
}

func newDebugController(session debugger.Session) *DebugController {
	return &DebugController{session: session}
}

func (c *DebugController) Start(ctx context.Context) (*debugger.Event, error) {
	return panicboundary.Call(func() (*debugger.Event, error) {
		return c.session.Start(ctx)
	})
}

func (c *DebugController) Continue(ctx context.Context) (*debugger.Event, error) {
	return panicboundary.Call(func() (*debugger.Event, error) {
		return c.session.Continue(ctx)
	})
}

func (c *DebugController) Next(ctx context.Context) (*debugger.Event, error) {
	return panicboundary.Call(func() (*debugger.Event, error) {
		return c.session.Next(ctx)
	})
}

func (c *DebugController) Step(ctx context.Context) (*debugger.Event, error) {
	return panicboundary.Call(func() (*debugger.Event, error) {
		return c.session.Step(ctx)
	})
}

func (c *DebugController) Out(ctx context.Context) (*debugger.Event, error) {
	return panicboundary.Call(func() (*debugger.Event, error) {
		return c.session.Out(ctx)
	})
}

func (c *DebugController) Pause() error {
	return panicboundary.Do(c.session.Pause)
}

func (c *DebugController) Frames() ([]debugger.Frame, error) {
	values, err := panicboundary.Call(c.session.Frames)
	if err != nil {
		return nil, err
	}

	return append([]debugger.Frame(nil), values...), nil
}

func (c *DebugController) FrameLocals(frame int) ([]debugger.Variable, error) {
	values, err := panicboundary.Call(func() ([]debugger.Variable, error) {
		return c.session.FrameLocals(frame)
	})
	if err != nil {
		return nil, err
	}

	return append([]debugger.Variable(nil), values...), nil
}

func (c *DebugController) Variables(reference debugger.ValueReference) ([]debugger.Variable, error) {
	values, err := panicboundary.Call(func() ([]debugger.Variable, error) {
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
	value, err := panicboundary.Call(func() (debugger.Value, error) {
		return c.session.EvaluateFrame(ctx, frame, expression)
	})

	return value, err
}

func (c *DebugController) SetBreakpoint(
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	value, err := panicboundary.Call(func() (debugger.Breakpoint, error) {
		return c.session.SetBreakpointAt(location, options)
	})

	return value, err
}

func (c *DebugController) DeleteBreakpoint(id debugger.BreakpointID) error {
	return panicboundary.Do(func() error {
		return c.session.DeleteBreakpoint(id)
	})
}

func (c *DebugController) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = runtimePanicError("close runtime debug session", panicboundary.Do(c.session.Close))
	})

	return c.closeErr
}
