package grpcserver

import wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"

// Handshake is immutable protocol and host-identity metadata supplied by the
// server composition root. RuntimeVersion belongs to host identity, not UAPI Version.
type Handshake struct {
	ProtocolName      string
	ProtocolVersion   string
	RuntimeName       string
	RuntimeVersion    string
	RuntimeInstanceID string
}

func protocolInfo(value Handshake) *wirev1.ProtocolInfo {
	return &wirev1.ProtocolInfo{Name: value.ProtocolName, Version: value.ProtocolVersion}
}

func runtimeIdentity(value Handshake) *wirev1.RuntimeIdentity {
	if value.RuntimeName == "" {
		return nil
	}

	return &wirev1.RuntimeIdentity{
		Name: value.RuntimeName, Version: value.RuntimeVersion, InstanceId: value.RuntimeInstanceID,
	}
}
