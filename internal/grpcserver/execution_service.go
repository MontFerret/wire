package grpcserver

import (
	"context"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/internal/core"
)

func (s *Server) Execute(ctx context.Context, request *wirev1.ExecuteRequest) (*wirev1.ExecuteResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	parameters, err := decodeParameters(request.GetParameters())
	if err != nil {
		return nil, rpcError(&core.DomainError{Category: core.ErrorInvalidRequest, Message: err.Error()})
	}

	snapshot, err := connection.Execute(ctx, core.ExecuteInput{
		PlanID:            core.PlanID(request.GetPlanId().GetValue()),
		Parameters:        parameters,
		OutputContentType: request.GetOutputContentType(),
	})
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ExecuteResponse{Execution: execution(snapshot)}, nil
}

func (s *Server) CancelExecution(_ context.Context, request *wirev1.CancelExecutionRequest) (*wirev1.CancelExecutionResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	snapshot, err := connection.CancelExecution(core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.CancelExecutionResponse{Execution: execution(snapshot)}, nil
}

func (s *Server) ReleaseExecution(ctx context.Context, request *wirev1.ReleaseExecutionRequest) (*wirev1.ReleaseExecutionResponse, error) {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return nil, err
	}

	if err := connection.ReleaseExecution(ctx, core.ExecutionID(request.GetExecutionId().GetValue())); err != nil {
		return nil, rpcError(err)
	}

	return &wirev1.ReleaseExecutionResponse{}, nil
}

func (s *Server) WatchExecution(request *wirev1.WatchExecutionRequest, stream wirev1.ExecutionService_WatchExecutionServer) error {
	connection, err := s.connection(request.GetConnectionId())
	if err != nil {
		return err
	}

	subscription, err := connection.WatchExecution(core.ExecutionID(request.GetExecutionId().GetValue()))
	if err != nil {
		return rpcError(err)
	}
	defer subscription.Cancel()

	if subscription.Current.Sequence > 0 {
		if err := stream.Send(executionEvent(subscription.Current)); err != nil {
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

			if err := stream.Send(executionEvent(event)); err != nil {
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

func subscriptionError(errors <-chan error) error {
	select {
	case err, ok := <-errors:
		if ok && err != nil {
			return rpcError(err)
		}
	default:
	}

	return nil
}
