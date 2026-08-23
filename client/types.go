// Package client provides a domain-oriented Ferret Wire client over a
// caller-owned gRPC connection.
package client

import (
	"google.golang.org/grpc/codes"
)

type (
	ConnectionID   string
	PlanID         string
	ExecutionID    string
	DebugSessionID string

	ClientIdentity struct {
		Name    string
		Version string
	}

	RuntimeIdentity struct {
		Name       string
		Version    string
		InstanceID string
	}

	Capabilities struct {
		Execution    bool
		Debugging    bool
		Cancellation bool
	}

	RuntimeInfo struct {
		APIIdentity     string
		WireVersion     string
		FerretVersion   string
		RuntimeIdentity *RuntimeIdentity
		Capabilities    Capabilities
	}

	Source struct {
		Content  string
		Identity string
	}

	CompileOptions struct {
		Debuggable bool
	}

	Plan struct {
		ID         PlanID
		Parameters []string
		Debuggable bool
	}

	ExecuteOptions struct {
		OutputContentType string
	}

	DebugSessionOptions struct {
		OutputContentType string
	}

	Parameters map[string]any

	Output struct {
		ContentType string
		Data        []byte
	}

	DiagnosticSpan struct {
		Start   uint64
		End     uint64
		Label   string
		Primary bool
	}

	Diagnostic struct {
		Kind           string
		Message        string
		Hint           string
		Note           string
		SourceIdentity string
		Spans          []DiagnosticSpan
	}

	Failure struct {
		Category    ErrorCategory
		Message     string
		Diagnostics []Diagnostic
	}

	ExecutionState uint8

	Execution struct {
		ID      ExecutionID
		PlanID  PlanID
		State   ExecutionState
		Output  *Output
		Failure *Failure
	}

	ExecutionEventKind uint8

	ExecutionEvent struct {
		ExecutionID ExecutionID
		Sequence    uint64
		Kind        ExecutionEventKind
		Execution   Execution
		Output      *Output
	}

	DebugState      uint8
	DebugStopReason uint8
	DebugEventKind  uint8
	ScopeKind       uint8

	Location struct {
		File   string
		Line   int
		Column int
	}

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

	DebugEvent struct {
		SessionID DebugSessionID
		Sequence  uint64
		Kind      DebugEventKind
		Session   DebugSession
		Output    *Output
	}

	BreakpointLocation struct {
		Line   int
		Column int
	}

	Breakpoint struct {
		ID              uint64
		File            string
		RequestedLine   int
		RequestedColumn int
		Line            int
		Column          int
		Verified        bool
	}

	DebugValue struct {
		Type      string
		Display   string
		Reference uint64
	}

	Variable struct {
		Name    string
		Value   DebugValue
		Mutable bool
	}

	StackFrame struct {
		Index    int
		Name     string
		Location *Location
	}

	Scope struct {
		Kind      ScopeKind
		Name      string
		Variables []Variable
	}

	ErrorCategory uint8

	Error struct {
		Code        codes.Code
		Category    ErrorCategory
		Message     string
		ResourceID  string
		Diagnostics []Diagnostic
		cause       error
	}
)

const (
	ExecutionRunning ExecutionState = iota + 1
	ExecutionCompleted
	ExecutionFailed
	ExecutionCancelled
)

const (
	ExecutionEventStarted ExecutionEventKind = iota + 1
	ExecutionEventOutput
	ExecutionEventCompleted
	ExecutionEventFailed
	ExecutionEventCancelled
)

const (
	DebugCreated DebugState = iota + 1
	DebugRunning
	DebugStopped
	DebugCompleted
	DebugFailed
	DebugTerminated
)

const (
	DebugStopNone DebugStopReason = iota
	DebugStopEntry
	DebugStopBreakpoint
	DebugStopStep
	DebugStopPause
	DebugStopRuntimeError
)

const (
	DebugEventStarted DebugEventKind = iota + 1
	DebugEventContinued
	DebugEventStopped
	DebugEventOutput
	DebugEventCompleted
	DebugEventFailed
	DebugEventTerminated
)

const (
	ScopeLocals ScopeKind = iota + 1
	ScopeParameters
)

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
)
