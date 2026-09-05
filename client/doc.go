// Package client provides a domain-oriented Ferret Wire client over a
// caller-owned gRPC connection.
//
// Runtime and Session alias the canonical github.com/MontFerret/api interfaces.
// Output aliases api.Output, defined by github.com/MontFerret/api/result.
// NewRuntime returns a private implementation over one logical Wire connection;
// its plan, normal-session, and debugger adapters also remain private. Client
// exposes the lower-level Plan, Execution, and DebugSession handles. Callers
// close resources explicitly, while protocol resource identifiers remain
// private to both facades.
//
// Runtime.Run invokes the hosted runtime directly. Client.Run is the lower-level
// one-shot composition: it compiles source, executes it, waits for encoded
// output, and releases the temporary resources. Callers that need Wire-specific
// events use the explicit Client, Plan, Execution, and DebugSession handles.
package client
