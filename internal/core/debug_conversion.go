package core

import "github.com/MontFerret/ferret/v2"

func debugStateName(state DebugState) string {
	switch state {
	case DebugCreated:
		return "created"
	case DebugRunning:
		return "running"
	case DebugStopped:
		return "stopped"
	case DebugCompleted:
		return "completed"
	case DebugFailed:
		return "failed"
	case DebugTerminated:
		return "terminated"
	default:
		return "unknown"
	}
}

func convertOutput(output *ferret.Output) *Output {
	if output == nil {
		return nil
	}

	return &Output{ContentType: output.ContentType, Content: append([]byte(nil), output.Content...)}
}

func convertDebugLocation(value ferret.DebugLocation) Location {
	return Location{File: value.File, Line: value.Line, Column: value.Column}
}

func convertBreakpoint(value ferret.DebugBreakpoint) Breakpoint {
	return Breakpoint{
		ID:              uint64(value.ID),
		File:            value.File,
		RequestedLine:   value.RequestedLine,
		RequestedColumn: value.RequestedColumn,
		Line:            value.Line,
		Column:          value.Column,
		Verified:        value.Bound,
	}
}

func convertVariable(value ferret.DebugVariable) Variable {
	return Variable{
		Name:      value.Name,
		Value:     convertDebugValue(value.Value),
		Mutable:   value.Mutable,
		Parameter: value.Param,
	}
}

func convertDebugValue(value ferret.DebugValue) DebugValue {
	return DebugValue{Type: value.Type, Display: value.Display, Reference: uint64(value.Reference)}
}
