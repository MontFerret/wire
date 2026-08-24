// Package client provides a domain-oriented Ferret Wire client over a
// caller-owned gRPC connection.
package client

import (
	"google.golang.org/grpc/codes"
)

type (
	// PlanID is an opaque server-issued plan identifier scoped to one Client.
	PlanID string

	// ExecutionID is an opaque server-issued execution identifier scoped to one Client.
	ExecutionID string

	// DebugSessionID is an opaque server-issued debug identifier scoped to one Client.
	DebugSessionID string

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

	// Source is FQL content plus its diagnostic and debugger identity.
	Source struct {
		Content  string
		Identity string
	}

	// CompileOptions controls Ferret plan construction.
	CompileOptions struct {
		Debuggable bool
	}

	// Plan is a compiled, connection-owned Ferret plan snapshot.
	Plan struct {
		ID         PlanID
		Parameters []string
		Debuggable bool
	}

	// ExecuteOptions controls encoded execution output.
	ExecuteOptions struct {
		OutputContentType string
	}

	// DebugSessionOptions controls encoded debug completion output.
	DebugSessionOptions struct {
		OutputContentType string
	}

	// Parameters is the explicit Wire parameter model accepted by Execute and
	// OpenDebugSession. Unsupported Go values are rejected locally.
	Parameters map[string]any

	// Output preserves Ferret's encoded content-type and byte abstraction.
	Output struct {
		ContentType string
		Content     []byte
	}

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

	// Failure is a sanitized terminal execution or debug failure.
	Failure struct {
		Category    ErrorCategory
		Message     string
		Diagnostics []Diagnostic
	}

	// ExecutionState describes the lifecycle state in an Execution snapshot.
	ExecutionState uint8

	// Execution is the current snapshot of one published execution.
	Execution struct {
		ID      ExecutionID
		PlanID  PlanID
		State   ExecutionState
		Output  *Output
		Failure *Failure
	}

	// ExecutionEventKind identifies an ordered execution state transition.
	ExecutionEventKind uint8

	// ExecutionEvent carries an ordered execution snapshot.
	ExecutionEvent struct {
		ExecutionID ExecutionID
		Sequence    uint64
		Kind        ExecutionEventKind
		Execution   Execution
	}

	// DebugState describes the lifecycle state in a DebugSession snapshot.
	DebugState uint8

	// DebugStopReason identifies why a running session became stopped.
	DebugStopReason uint8

	// DebugEventKind identifies an ordered debug state transition.
	DebugEventKind uint8

	// Location is a Ferret source position. Breakpoint column zero is unspecified.
	Location struct {
		File   string
		Line   int
		Column int
	}

	// DebugSession is the current snapshot of one Ferret debug session.
	DebugSession struct {
		ID               DebugSessionID
		PlanID           PlanID
		State            DebugState
		StopReason       DebugStopReason
		Location         *Location
		HitBreakpointIDs []uint64
		Output           *Output
		Failure          *Failure
	}

	// DebugEvent carries an ordered debug-session snapshot.
	DebugEvent struct {
		SessionID DebugSessionID
		Sequence  uint64
		Kind      DebugEventKind
		Session   DebugSession
	}

	// Breakpoint describes the requested and bound Ferret breakpoint locations.
	Breakpoint struct {
		ID              uint64
		File            string
		RequestedLine   int
		RequestedColumn int
		Line            int
		Column          int
		Verified        bool
	}

	// DebugValue is Ferret's formatted debugger value and optional expansion reference.
	DebugValue struct {
		Type      string
		Display   string
		Reference uint64
	}

	// Variable is a Ferret debugger variable. Parameter distinguishes declared
	// query parameters from other frame locals.
	Variable struct {
		Name      string
		Value     DebugValue
		Mutable   bool
		Parameter bool
	}

	// Frame describes one paused frame and its zero-based inspection index.
	Frame struct {
		Index    int
		Name     string
		Location *Location
	}

	// ErrorCategory is the stable Wire failure category independent of gRPC code.
	ErrorCategory uint8

	// Error is a structured Wire RPC failure. Internal causes remain available
	// through Unwrap without being copied into Message.
	Error struct {
		Code        codes.Code
		Category    ErrorCategory
		Message     string
		ResourceID  string
		Diagnostics []Diagnostic
		cause       error
	}
)

// Execution lifecycle states.
const (
	ExecutionRunning ExecutionState = iota + 1
	ExecutionCompleted
	ExecutionFailed
	ExecutionCancelled
)

// Execution event kinds. Every execution has one ordered terminal event.
const (
	ExecutionEventStarted ExecutionEventKind = iota + 1
	ExecutionEventCompleted
	ExecutionEventFailed
	ExecutionEventCancelled
)

// Debug session lifecycle states.
const (
	DebugCreated DebugState = iota + 1
	DebugRunning
	DebugStopped
	DebugCompleted
	DebugFailed
	DebugTerminated
)

// Debug stop reasons reported by Ferret.
const (
	DebugStopNone DebugStopReason = iota
	DebugStopEntry
	DebugStopBreakpoint
	DebugStopStep
	DebugStopPause
	DebugStopRuntimeError
)

// Debug event kinds. Every session has one ordered terminal event.
const (
	DebugEventStarted DebugEventKind = iota + 1
	DebugEventContinued
	DebugEventStopped
	DebugEventCompleted
	DebugEventFailed
	DebugEventTerminated
)

// Structured Wire error categories.
const (
	ErrorInvalidRequest ErrorCategory = iota + 1
	ErrorCompilation
	ErrorExecution
	ErrorPlanNotFound
	ErrorExecutionNotFound
	ErrorDebugSessionNotFound
	ErrorConnectionNotFound
	ErrorInvalidState
	ErrorUnsupported
	ErrorInternal
	ErrorWatcherLagged
	ErrorCancelled
	ErrorValueReferenceNotFound
	ErrorResourceExhausted
	ErrorBreakpointNotFound
)
