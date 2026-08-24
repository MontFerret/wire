package client

// Parameters is the explicit Wire parameter model accepted by Plan.Execute
// and Plan.NewDebugSession. Unsupported Go values are rejected locally.
type Parameters map[string]any
