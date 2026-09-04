package client

import (
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/execution"
)

type (
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
		RuntimeIdentity *execution.Identity
		Capabilities    Capabilities
	}
)

func convertRuntimeInfo(protocol *wirev1.ProtocolInfo, identity *wirev1.RuntimeIdentity) RuntimeInfo {
	result := RuntimeInfo{
		APIIdentity: protocol.GetName(),
		WireVersion: protocol.GetVersion(),
	}

	if identity != nil {
		result.RuntimeIdentity = &execution.Identity{
			Name:       identity.GetName(),
			Version:    identity.GetVersion(),
			InstanceID: identity.GetInstanceId(),
		}
	}

	return result
}
