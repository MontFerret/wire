package client

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
