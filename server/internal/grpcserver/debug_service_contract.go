package grpcserver

import (
	"context"

	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

// RunCommand preserves command contexts and carries events independently of errors.
// A Start stream owns the execution context, unlike a non-owning WatchDebug stream.
func (s *DebugService) RunCommand(request *wirev1.RunCommandRequest, stream wirev1.DebugService_RunCommandServer) error {
	operation, cancel, session, err := s.debugCommand(stream.Context(), request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return err
	}
	defer cancel()

	release, err := session.ReserveCommandStream()
	if err != nil {
		return rpcError(err)
	}
	defer release()
	var name string
	switch request.GetCommand() {
	case wirev1.DebugCommand_DEBUG_COMMAND_START:
		name = "start"
	case wirev1.DebugCommand_DEBUG_COMMAND_CONTINUE:
		name = "continue"
	case wirev1.DebugCommand_DEBUG_COMMAND_STEP_IN:
		name = "step-in"
	case wirev1.DebugCommand_DEBUG_COMMAND_STEP_OVER:
		name = "step-over"
	case wirev1.DebugCommand_DEBUG_COMMAND_STEP_OUT:
		name = "step-out"
	default:
		return rpcError(&core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: "invalid debug command or position"})
	}

	command, err := session.Command(name)
	if err != nil {
		return rpcError(err)
	}

	result, err := session.RunCommand(operation, name == "start", command)
	if err != nil {
		return rpcError(err)
	}

	converted, err := commandResult(result)
	if err != nil {
		return rpcError(err)
	}

	if err := stream.Send(&wirev1.RunCommandResponse{Result: converted}); err != nil {
		return err
	}

	if name == "start" && result.Event != nil && result.Event.Reason != debugger.ReasonCompleted && result.Event.Reason != debugger.ReasonTerminated {
		select {
		case <-operation.Done():
			return rpcError(operation.Err())
		case <-session.Done():
		}
	}

	return nil
}

// ReplaceBreakpoints projects one source-wide atomic publication.
func (s *DebugService) ReplaceBreakpoints(ctx context.Context, request *wirev1.ReplaceBreakpointsRequest) (*wirev1.ReplaceBreakpointsResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}
	defer cancel()

	if len(request.GetRequests()) > session.BreakpointLimit() {
		return nil, rpcError(&core.DomainError{Kind: core.ErrorKindResourceExhausted, Message: "breakpoint limit reached"})
	}

	requests := make([]debugger.BreakpointRequest, len(request.GetRequests()))
	for i, value := range request.GetRequests() {
		if value.GetPosition() == nil {
			return nil, rpcError(&core.DomainError{Kind: core.ErrorKindInvalidRequest, Message: "invalid debug command or position"})
		}

		line, err := intFromProto(value.GetPosition().GetLine(), "breakpoint line")
		if err != nil {
			return nil, rpcError(err)
		}

		column, err := intFromProto(value.GetPosition().GetColumn(), "breakpoint column")
		if err != nil {
			return nil, rpcError(err)
		}

		options, err := breakpointOptions(value.GetOptions())
		if err != nil {
			return nil, rpcError(err)
		}

		requests[i] = debugger.BreakpointRequest{Position: source.Position{Line: line, Column: column}, Options: options}
	}

	values, err := session.ReplaceBreakpoints(operation, request.GetSourceName(), requests)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := breakpointsToProto(values)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReplaceBreakpointsResponse{Breakpoints: converted}, nil
}

// Breakpoints enumerates the complete hosted snapshot, including after termination.
func (s *DebugService) Breakpoints(ctx context.Context, request *wirev1.BreakpointsRequest) (*wirev1.BreakpointsResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}
	defer cancel()

	values, err := session.Breakpoints(operation)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := breakpointsToProto(values)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.BreakpointsResponse{Breakpoints: converted}, nil
}

// Locals preserves the hosted default-frame operation.
func (s *DebugService) Locals(ctx context.Context, request *wirev1.LocalsRequest) (*wirev1.LocalsResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}
	defer cancel()

	values, err := session.Locals(operation)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := variablesToProto(values)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.LocalsResponse{Variables: converted}, nil
}

// Evaluate preserves the hosted default-frame operation.
func (s *DebugService) Evaluate(ctx context.Context, request *wirev1.EvaluateRequest) (*wirev1.EvaluateResponse, error) {
	operation, cancel, session, err := s.debugCommand(ctx, request.GetConnectionId(), request.GetDebugSessionId())
	if err != nil {
		return nil, err
	}
	defer cancel()

	value, err := session.Evaluate(operation, request.GetExpression())
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := debugValue(value)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.EvaluateResponse{Value: converted}, nil
}
