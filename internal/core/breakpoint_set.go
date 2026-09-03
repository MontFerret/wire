package core

import (
	"fmt"

	"github.com/MontFerret/api/debugger"
)

type breakpointSet struct {
	limit  int
	values map[debugger.BreakpointID]debugger.Breakpoint
}

func newBreakpointSet(limit int) *breakpointSet {
	return &breakpointSet{
		limit:  limit,
		values: make(map[debugger.BreakpointID]debugger.Breakpoint),
	}
}

func (s *breakpointSet) checkCapacity() error {
	if len(s.values) >= s.limit {
		return resourceExhausted("breakpoint limit reached")
	}

	return nil
}

func (s *breakpointSet) get(id debugger.BreakpointID) (debugger.Breakpoint, error) {
	value, exists := s.values[id]
	if !exists {
		return debugger.Breakpoint{}, notFound(ErrorBreakpointNotFound, fmt.Sprint(id))
	}

	return value, nil
}

func (s *breakpointSet) add(value debugger.Breakpoint) {
	s.values[value.ID] = value
}

func (s *breakpointSet) delete(id debugger.BreakpointID) {
	delete(s.values, id)
}
