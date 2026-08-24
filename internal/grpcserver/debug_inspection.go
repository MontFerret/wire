package grpcserver

import (
	"context"

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
		result[i] = &wirev1.Frame{Index: int32(value.Index), Name: value.Name, Location: location(value.Location)}
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
		result[i] = variable(value)
	}

	return &wirev1.FrameLocalsResponse{Variables: result}, nil
}

func (s *Server) Variables(ctx context.Context, request *wirev1.VariablesRequest) (*wirev1.VariablesResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	values, err := connection.Variables(ctx, core.DebugSessionID(request.GetDebugSessionId().GetValue()), request.GetReference())
	if err != nil {
		return nil, rpcError(err)
	}

	result := make([]*wirev1.Variable, len(values))
	for i, value := range values {
		result[i] = variable(value)
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

	return &wirev1.EvaluateFrameResponse{Value: debugValue(value)}, nil
}
