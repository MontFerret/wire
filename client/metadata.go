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
