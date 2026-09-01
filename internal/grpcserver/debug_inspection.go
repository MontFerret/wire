package grpcserver

import (
	"context"

	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

func (s *Server) Frames(ctx context.Context, request *wirev1.FramesRequest) (*wirev1.FramesResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	values, err := connection.Frames(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	result := make([]*wirev1.Frame, len(values))
	for i, value := range values {
		converted, err := frame(value)
		if err != nil {
			return nil, rpcError(err)
		}
		result[i] = converted
	}

	return &wirev1.FramesResponse{Frames: result}, nil
}

func (s *Server) FrameLocals(ctx context.Context, request *wirev1.FrameLocalsRequest) (*wirev1.FrameLocalsResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	values, err := connection.FrameLocals(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), int(request.GetFrameIndex()))
	if err != nil {
		return nil, rpcError(err)
	}

	result := make([]*wirev1.Variable, len(values))
	for i, value := range values {
		converted, err := variable(value)
		if err != nil {
			return nil, rpcError(err)
		}
		result[i] = converted
	}

	return &wirev1.FrameLocalsResponse{Variables: result}, nil
}

func (s *Server) Variables(ctx context.Context, request *wirev1.VariablesRequest) (*wirev1.VariablesResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	reference, err := debuggerIDFromProto[debugger.ValueReference](request.GetReference(), "value reference")
	if err != nil {
		return nil, rpcError(err)
	}

	values, err := connection.Variables(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), reference)
	if err != nil {
		return nil, rpcError(err)
	}

	result := make([]*wirev1.Variable, len(values))
	for i, value := range values {
		converted, err := variable(value)
		if err != nil {
			return nil, rpcError(err)
		}
		result[i] = converted
	}

	return &wirev1.VariablesResponse{Variables: result}, nil
}

func (s *Server) EvaluateFrame(ctx context.Context, request *wirev1.EvaluateFrameRequest) (*wirev1.EvaluateFrameResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	value, err := connection.EvaluateFrame(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), int(request.GetFrameIndex()), request.GetExpression())
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := debugValue(value)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.EvaluateFrameResponse{Value: converted}, nil
}
