package grpcserver

import (
	"context"

	"github.com/MontFerret/api/debugger"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/server/internal/core"
)

func (s *DebugService) Frames(ctx context.Context, request *wirev1.FramesRequest) (*wirev1.FramesResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	session, err := resources.DebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	values, err := session.Frames(operation)
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

func (s *DebugService) FrameLocals(ctx context.Context, request *wirev1.FrameLocalsRequest) (*wirev1.FrameLocalsResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	session, err := resources.DebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	values, err := session.FrameLocals(operation, int(request.GetFrameIndex()))
	if err != nil {
		return nil, rpcError(err)
	}

	result, err := variablesToProto(values)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.FrameLocalsResponse{Variables: result}, nil
}

func (s *DebugService) Variables(ctx context.Context, request *wirev1.VariablesRequest) (*wirev1.VariablesResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	reference, err := debuggerIDFromProto[debugger.ValueReference](request.GetReference(), "value reference")
	if err != nil {
		return nil, rpcError(err)
	}

	session, err := resources.DebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	values, err := session.Variables(operation, reference)
	if err != nil {
		return nil, rpcError(err)
	}

	result, err := variablesToProto(values)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.VariablesResponse{Variables: result}, nil
}

func (s *DebugService) EvaluateFrame(ctx context.Context, request *wirev1.EvaluateFrameRequest) (*wirev1.EvaluateFrameResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	session, err := resources.DebugSession(operation, core.DebugSessionID(request.GetDebugSessionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	value, err := session.EvaluateFrame(operation, int(request.GetFrameIndex()), request.GetExpression())
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := debugValue(value)
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.EvaluateFrameResponse{Value: converted}, nil
}
