package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type breakpointSet struct {
	mu      sync.Mutex
	session debugger.Session
	limit   int
	values  map[debugger.BreakpointID]debugger.Breakpoint
}

func newBreakpointSet(session debugger.Session, limit int) *breakpointSet {
	return &breakpointSet{
		session: session,
		limit:   limit,
		values:  make(map[debugger.BreakpointID]debugger.Breakpoint),
	}
}

func (s *breakpointSet) set(
	ctx context.Context,
	location source.Location,
	options debugger.BreakpointOptions,
) (debugger.Breakpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.values) >= s.limit {
		return debugger.Breakpoint{}, resourceExhausted("breakpoint limit reached")
	}

	if err := ctx.Err(); err != nil {
		return debugger.Breakpoint{}, err
	}

	value, err := s.session.SetBreakpointAt(location, options)
	if err != nil {
		return debugger.Breakpoint{}, invalidState("set breakpoint failed", err)
	}

	s.values[value.ID] = value

	return value, nil
}

func (s *breakpointSet) delete(ctx context.Context, id debugger.BreakpointID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, exists := s.values[id]
	if !exists {
		return notFound(ErrorBreakpointNotFound, fmt.Sprint(id))
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := s.session.DeleteBreakpoint(value.ID); err != nil {
		return invalidState("delete breakpoint failed", err)
	}

	delete(s.values, id)

	return nil
}
