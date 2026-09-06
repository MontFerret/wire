package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	wireexecution "github.com/MontFerret/wire/pkg/execution"
	"github.com/MontFerret/wire/server/internal/core"
)

// ExecutionService adapts the execution RPC contract to its core owners.
type ExecutionService struct {
	wirev1.UnimplementedExecutionServiceServer
	connections *core.ConnectionRegistry
}

var _ wirev1.ExecutionServiceServer = (*ExecutionService)(nil)

func (s *ExecutionService) Execute(ctx context.Context, request *wirev1.ExecuteRequest) (*wirev1.ExecuteResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	options, err := decodeSessionOptions(request.GetParameters(), request.GetOutputContentType())
	if err != nil {
		return nil, rpcError(err)
	}

	parent, err := resources.Plan(operation, core.PlanID(request.GetPlanId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	created, err := parent.Execute(operation, options...)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := execution(created.ID(), created.Snapshot())
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ExecuteResponse{Execution: converted}, nil
}

func (s *ExecutionService) RunSession(
	ctx context.Context,
	request *wirev1.RunSessionRequest,
) (*wirev1.RunSessionResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	session, err := resources.Session(operation, core.SessionID(request.GetSessionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	created, err := session.Execute(operation)
	if err != nil {
		return nil, rpcError(err)
	}

	converted, err := execution(created.ID(), wireexecution.Snapshot{State: wireexecution.StateRunning})
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.RunSessionResponse{Execution: converted}, nil
}

func (s *ExecutionService) CancelExecution(ctx context.Context, request *wirev1.CancelExecutionRequest) (*wirev1.CancelExecutionResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	execution, err := resources.Execution(operation, core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	execution.Cancel()

	return &wirev1.CancelExecutionResponse{}, nil
}

func (s *ExecutionService) ReleaseExecution(ctx context.Context, request *wirev1.ReleaseExecutionRequest) (*wirev1.ReleaseExecutionResponse, error) {
	operation, resources, cancel, err := prepareOperation(ctx, s.connections, request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	defer cancel()

	if err := resources.ReleaseExecution(operation, core.ExecutionID(request.GetExecutionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseExecutionResponse{}, nil
}

func (s *ExecutionService) WatchExecution(request *wirev1.WatchExecutionRequest, stream wirev1.ExecutionService_WatchExecutionServer) error {
	operation, resources, cancel, err := prepareOperation(stream.Context(), s.connections, request.GetConnectionId())
	if err != nil {
		return err
	}

	defer cancel()

	execution, err := resources.Execution(operation, core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}

	subscription, err := execution.Watch()
	if err != nil {
		return rpcError(err)
	}

	defer subscription.Cancel()

	if subscription.Current.Sequence > 0 {
		converted, err := executionEvent(execution.ID(), subscription.Current)
		if err != nil {
			return rpcError(err)
		}

		if err := stream.Send(converted); err != nil {
			return err
		}
	}

	eventsChannel := subscription.Events
	errorsChannel := subscription.Errors

	for {
		select {
		case <-stream.Context().Done():
			return rpcError(stream.Context().Err())
		case event, ok := <-eventsChannel:
			if !ok {
				return subscriptionError(errorsChannel)
			}

			converted, err := executionEvent(execution.ID(), event)
			if err != nil {
				return rpcError(err)
			}

			if err := stream.Send(converted); err != nil {
				return err
			}
		case watchErr, ok := <-errorsChannel:
			if ok && watchErr != nil {
				return rpcError(watchErr)
			}

			if !ok {
				errorsChannel = nil
			}
		}
	}
}
