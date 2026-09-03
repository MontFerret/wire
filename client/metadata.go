package client

import wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"

type (
	// RuntimeIdentity describes the optional host application identity published
	// by the server.
	RuntimeIdentity struct {
		Name       string
		Version    string
		InstanceID string
	}

	// Capabilities reports the operation families supported by the server.
	Capabilities struct {
		Execution    bool
		Debugging    bool
		Cancellation bool
	}

	// RuntimeInfo is the immutable server metadata returned by the Connect handshake.
	RuntimeInfo struct {
		APIIdentity     string
		WireVersion     string
		FerretVersion   string
		RuntimeIdentity *RuntimeIdentity
		Capabilities    Capabilities
	}
)

func convertRuntimeInfo(protocol *wirev1.ProtocolInfo, identity *wirev1.RuntimeIdentity) RuntimeInfo {
	result := RuntimeInfo{
		APIIdentity: protocol.GetName(),
		WireVersion: protocol.GetVersion(),
	}

	if identity != nil {
		result.RuntimeIdentity = &RuntimeIdentity{
			Name:       identity.GetName(),
			Version:    identity.GetVersion(),
			InstanceID: identity.GetInstanceId(),
		}
	}

	return result
}
