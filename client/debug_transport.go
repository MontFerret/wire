package client

import (
	"context"
	"errors"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
)

// debugTransport performs debug-session-specific protocol operations.
type debugTransport struct {
	rpc     wirev1.DebugServiceClient
	session *session
}

func newDebugTransport(connection grpc.ClientConnInterface, session *session) *debugTransport {
	return &debugTransport{rpc: wirev1.NewDebugServiceClient(connection), session: session}
}

func (t *debugTransport) open(
	ctx context.Context,
	planID string,
	parameters Parameters,
	options DebugSessionOptions,
) (string, error) {
	converted, err := encodeParameters(parameters)
	if err != nil {
		return "", err
	}

	response, err := t.rpc.OpenDebugSession(ctx, &wirev1.OpenDebugSessionRequest{
		ConnectionId:      t.session.connectionProto(),
		PlanId:            &wirev1.PlanId{Value: planID},
		Parameters:        converted,
		OutputContentType: options.OutputContentType,
	})
	if err != nil {
		return "", decodeError(err)
	}

	value := response.GetSession()
	if value == nil || value.GetId().GetValue() == "" {
		return "", errors.New("Wire server returned an invalid debug session")
	}

	return value.GetId().GetValue(), nil
}

func (t *debugTransport) start(ctx context.Context, id string) error {
	_, err := t.rpc.StartDebug(ctx, &wirev1.StartDebugRequest{Command: t.command(id)})

	return decodeError(err)
}

func (t *debugTransport) continueExecution(ctx context.Context, id string) error {
	_, err := t.rpc.Continue(ctx, &wirev1.ContinueRequest{Command: t.command(id)})

	return decodeError(err)
}

func (t *debugTransport) pause(ctx context.Context, id string) error {
	_, err := t.rpc.Pause(ctx, &wirev1.PauseRequest{Command: t.command(id)})

	return decodeError(err)
}

func (t *debugTransport) next(ctx context.Context, id string) error {
	_, err := t.rpc.Next(ctx, &wirev1.NextRequest{Command: t.command(id)})

	return decodeError(err)
}

func (t *debugTransport) step(ctx context.Context, id string) error {
	_, err := t.rpc.Step(ctx, &wirev1.StepRequest{Command: t.command(id)})

	return decodeError(err)
}

func (t *debugTransport) out(ctx context.Context, id string) error {
	_, err := t.rpc.Out(ctx, &wirev1.OutRequest{Command: t.command(id)})

	return decodeError(err)
}

func (t *debugTransport) stop(ctx context.Context, id string) error {
	_, err := t.rpc.StopDebug(ctx, &wirev1.StopDebugRequest{Command: t.command(id)})

	return decodeError(err)
}

func (t *debugTransport) release(ctx context.Context, id string) error {
	_, err := t.rpc.ReleaseDebugSession(ctx, &wirev1.ReleaseDebugSessionRequest{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
	})

	return decodeError(err)
}

func (t *debugTransport) command(id string) *wirev1.DebugCommand {
	return &wirev1.DebugCommand{
		ConnectionId:   t.session.connectionProto(),
		DebugSessionId: &wirev1.DebugSessionId{Value: id},
	}
}
