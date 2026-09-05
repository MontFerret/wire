package core

import (
	"github.com/MontFerret/wire/pkg/execution"
)

type RuntimeInfo struct {
	ProtocolName    string
	ProtocolVersion string
	RuntimeIdentity execution.Identity
}
