package client

type (
	// DiagnosticSpan is retained until the client facade is rebuilt. Wire v1
	// does not transport diagnostics without a portable Unified API contract.
	DiagnosticSpan struct {
		Start   uint64
		End     uint64
		Label   string
		Primary bool
	}

	// Diagnostic is retained until the client facade is rebuilt. Diagnostic
	// slices returned by the current facade are always empty.
	Diagnostic struct {
		Kind           string
		Message        string
		Hint           string
		Note           string
		SourceIdentity string
		Spans          []DiagnosticSpan
	}
)
