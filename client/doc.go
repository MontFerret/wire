// Package client provides a domain-oriented Ferret Wire client over a
// caller-owned gRPC connection.
//
// A Client owns one logical Wire connection and the Plan handles compiled
// through it. Each Plan owns its Execution and DebugSession handles. Callers
// close those handles explicitly, while protocol resource identifiers remain
// private to the facade.
//
// Client.Run is the one-shot API: it compiles source, executes it, waits for
// encoded output, and releases the temporary resources. Callers that need to
// reuse a compiled program or observe execution and debugger events use the
// explicit Client, Plan, Execution, and DebugSession handles instead.
package client
