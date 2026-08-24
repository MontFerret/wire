package client

type (
	// DiagnosticSpan is a labeled half-open UTF-8 byte span in source.
	DiagnosticSpan struct {
		Start   uint64
		End     uint64
		Label   string
		Primary bool
	}

	// Diagnostic is a structured Ferret compiler or runtime diagnostic.
	Diagnostic struct {
		Kind           string
		Message        string
		Hint           string
		Note           string
		SourceIdentity string
		Spans          []DiagnosticSpan
	}
)
