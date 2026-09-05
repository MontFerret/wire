package client

// RunOptions composes plan compilation and execution options for Client.Run.
type RunOptions struct {
	// Compile controls construction of the temporary plan.
	Compile CompileOptions
	// Execute controls the temporary execution and its encoded output.
	Execute ExecuteOptions
}
