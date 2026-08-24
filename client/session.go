package client

import (
	"context"
	"errors"
	"io"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
)

// session performs the logical RuntimeService connection protocol and retains
// the opaque connection identity shared by capability-specific transports.
type session struct {
	rpc           wirev1.RuntimeServiceClient
	id            string
	connectStream wirev1.RuntimeService_ConnectClient
	connectCancel context.CancelFunc
}

func openSession(ctx context.Context, connection grpc.ClientConnInterface) (*session, RuntimeInfo, error) {
	runtimeClient := wirev1.NewRuntimeServiceClient(connection)
	streamCtx, streamCancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := runtimeClient.Connect(streamCtx, &wirev1.ConnectRequest{})
	if err != nil {
		streamCancel()

		return nil, RuntimeInfo{}, decodeError(err)
	}

	type firstResult struct {
		response *wirev1.ConnectResponse
		err      error
	}

	first := make(chan firstResult, 1)

	go func() {
		response, receiveErr := stream.Recv()
		first <- firstResult{response: response, err: receiveErr}
	}()

	var response *wirev1.ConnectResponse
	select {
	case <-ctx.Done():
		streamCancel()

		return nil, RuntimeInfo{}, ctx.Err()
	case result := <-first:
		if result.err != nil {
			streamCancel()

			return nil, RuntimeInfo{}, decodeError(result.err)
		}

		response = result.response
	}

	opened := response.GetOpened()
	if opened == nil || opened.GetConnectionId().GetValue() == "" || opened.GetRuntimeInfo() == nil {
		streamCancel()

		return nil, RuntimeInfo{}, errors.New("Wire server returned an invalid Connect handshake")
	}

	session := &session{
		rpc:           runtimeClient,
		id:            opened.GetConnectionId().GetValue(),
		connectStream: stream,
		connectCancel: streamCancel,
	}

	return session, convertRuntimeInfo(opened.GetRuntimeInfo()), nil
}

func (s *session) monitor() error {
	_, err := s.connectStream.Recv()
	if err == nil {
		return errors.New("Wire server returned an unexpected Connect response")
	}

	if errors.Is(err, io.EOF) {
		return nil
	}

	return decodeError(err)
}

func (s *session) close(ctx context.Context) error {
	_, err := s.rpc.CloseConnection(ctx, &wirev1.CloseConnectionRequest{
		ConnectionId: s.connectionProto(),
	})

	return decodeError(err)
}

func (s *session) cancel() {
	s.connectCancel()
}

func (s *session) connectionProto() *wirev1.ConnectionId {
	return &wirev1.ConnectionId{Value: s.id}
}

func convertRuntimeInfo(value *wirev1.RuntimeInfo) RuntimeInfo {
	result := RuntimeInfo{
		APIIdentity:   value.GetApiIdentity(),
		WireVersion:   value.GetWireVersion(),
		FerretVersion: value.GetFerretVersion(),
	}

	if identity := value.GetRuntimeIdentity(); identity != nil {
		result.RuntimeIdentity = &RuntimeIdentity{
			Name:       identity.GetName(),
			Version:    identity.GetVersion(),
			InstanceID: identity.GetInstanceId(),
		}
	}

	for _, capability := range value.GetCapabilities() {
		switch capability {
		case wirev1.Capability_CAPABILITY_EXECUTION:
			result.Capabilities.Execution = true
		case wirev1.Capability_CAPABILITY_DEBUGGING:
			result.Capabilities.Debugging = true
		case wirev1.Capability_CAPABILITY_CANCELLATION:
			result.Capabilities.Cancellation = true
		}
	}

	return result
}
