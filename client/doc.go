// Package client provides a domain-oriented Ferret Wire client over a
// caller-owned gRPC connection.
//
// Runtime implements the Universal Ferret API over one logical Wire connection.
// Its Plan, Session, and debugger adapters remain private behind the API
// interfaces. Client exposes the lower-level Plan, Execution, and DebugSession
// handles. Callers close resources explicitly, while protocol resource
// identifiers remain private to both facades.
//
// Runtime.Run invokes the hosted runtime directly. Client.Run is the lower-level
// one-shot composition: it compiles source, executes it, waits for encoded
// output, and releases the temporary resources. Callers that need Wire-specific
// events use the explicit Client, Plan, Execution, and DebugSession handles.
package client
