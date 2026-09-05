package harness

import (
	"context"
	"sync"

	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type (
	// Operation names faults without exposing protobuf services to contract tests.
	Operation string
	Outcome   string

	ResponseGate struct {
		Committed chan struct{}
		deliver   chan struct{}
		once      sync.Once
		outcome   Outcome
	}

	// Faults forwards real RPCs. Allocation faults only alter already received replies.
	Faults struct {
		grpc.ClientConnInterface
		mu               sync.Mutex
		gates            map[Operation]*ResponseGate
		allGates         []*ResponseGate
		failures         map[Operation]error
		responseFailures map[Operation]error
		watchFailures    map[Operation]watchFailure
		sequence         []Operation
	}

	watchFailure struct {
		err   error
		after <-chan struct{}
	}

	failingStream struct {
		grpc.ClientStream
		cancel   context.CancelFunc
		err      error
		received bool
		after    <-chan struct{}
	}
)

const (
	Compile          Operation = "compile"
	CompileDebug     Operation = "compile debug"
	CreateSession    Operation = "session"
	CreateDebugger   Operation = "debug session"
	RunSession       Operation = "session run"
	RunRuntime       Operation = "runtime run"
	ReleasePlan      Operation = "release plan"
	ReleaseSession   Operation = "release session"
	ReleaseDebugger  Operation = "release debugger"
	ReleaseExecution Operation = "release execution"
	CloseRuntime     Operation = "close runtime"
	CancelExecution  Operation = "cancel execution"
	WatchExecution   Operation = "watch execution"
	WatchDebugger    Operation = "watch debugger"

	Deliver         Outcome = "success"
	LostDeadline    Outcome = "deadline"
	LostUnavailable Outcome = "unavailable"
	LostOversized   Outcome = "oversized"
	LostDecode      Outcome = "transport internal"
	Malformed       Outcome = "malformed"
)

func operationFor(method string) Operation {
	switch method {
	case wirev1.PlanService_Compile_FullMethodName:
		return Compile
	case wirev1.PlanService_CompileDebug_FullMethodName:
		return CompileDebug
	case wirev1.SessionService_CreateSession_FullMethodName:
		return CreateSession
	case wirev1.DebugService_CreateDebugSession_FullMethodName:
		return CreateDebugger
	case wirev1.ExecutionService_RunSession_FullMethodName:
		return RunSession
	case wirev1.RuntimeService_Run_FullMethodName:
		return RunRuntime
	case wirev1.PlanService_ReleasePlan_FullMethodName:
		return ReleasePlan
	case wirev1.SessionService_ReleaseSession_FullMethodName:
		return ReleaseSession
	case wirev1.DebugService_ReleaseDebugSession_FullMethodName:
		return ReleaseDebugger
	case wirev1.ExecutionService_ReleaseExecution_FullMethodName:
		return ReleaseExecution
	case wirev1.RuntimeService_CloseConnection_FullMethodName:
		return CloseRuntime
	case wirev1.ExecutionService_CancelExecution_FullMethodName:
		return CancelExecution
	case wirev1.ExecutionService_WatchExecution_FullMethodName:
		return WatchExecution
	case wirev1.DebugService_WatchDebug_FullMethodName:
		return WatchDebugger
	default:
		return Operation(method)
	}
}

func newFaults(connection grpc.ClientConnInterface) *Faults {
	return &Faults{
		ClientConnInterface: connection,
		gates:               make(map[Operation]*ResponseGate),
		failures:            make(map[Operation]error),
		responseFailures:    make(map[Operation]error),
		watchFailures:       make(map[Operation]watchFailure),
	}
}

func (f *Faults) Arm(operation Operation, outcome Outcome) *ResponseGate {
	f.mu.Lock()
	defer f.mu.Unlock()

	gate := &ResponseGate{Committed: make(chan struct{}), deliver: make(chan struct{}), outcome: outcome}
	f.gates[operation] = gate
	f.allGates = append(f.allGates, gate)

	return gate
}

func (g *ResponseGate) Deliver() {
	g.once.Do(func() { close(g.deliver) })
}

func (f *Faults) Invoke(ctx context.Context, method string, request, response any, options ...grpc.CallOption) error {
	operation := operationFor(method)
	f.mu.Lock()
	f.sequence = append(f.sequence, operation)
	gate, failure, responseFailure := f.gates[operation], f.failures[operation], f.responseFailures[operation]
	delete(f.gates, operation)
	f.mu.Unlock()

	if failure != nil {
		return failure
	}

	if err := f.ClientConnInterface.Invoke(ctx, method, request, response, options...); err != nil {
		return err
	}

	if responseFailure != nil {
		return responseFailure
	}

	if gate == nil {
		return nil
	}

	close(gate.Committed)

	select {
	case <-gate.deliver:
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}

	switch gate.outcome {
	case LostDeadline:
		return status.Error(codes.DeadlineExceeded, "allocation response lost")
	case LostUnavailable:
		return status.Error(codes.Unavailable, "allocation response lost")
	case LostOversized:
		return status.Error(codes.ResourceExhausted, "allocation response exceeds receive limit")
	case LostDecode:
		return status.Error(codes.Internal, "allocation response could not be decoded")
	case Malformed:
		proto.Reset(response.(proto.Message))
	}

	return nil
}

func (f *Faults) NewStream(ctx context.Context, description *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	f.mu.Lock()
	failure, injected := f.watchFailures[operationFor(method)]
	delete(f.watchFailures, operationFor(method))
	f.mu.Unlock()

	if !injected {
		return f.ClientConnInterface.NewStream(ctx, description, method, options...)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := f.ClientConnInterface.NewStream(streamCtx, description, method, options...)
	if err != nil {
		cancel()

		return nil, err
	}

	return &failingStream{ClientStream: stream, cancel: cancel, err: failure.err, after: failure.after}, nil
}

func (s *failingStream) RecvMsg(message any) error {
	if s.received {
		if s.after != nil {
			select {
			case <-s.after:
			case <-s.Context().Done():
				return status.FromContextError(s.Context().Err()).Err()
			}
		}

		s.cancel()

		return s.err
	}

	s.received = true

	err := s.ClientStream.RecvMsg(message)
	if err != nil {
		s.cancel()
	}

	return err
}

func (f *Faults) Fail(operation Operation, err error) {
	f.mu.Lock()
	f.failures[operation] = err
	f.mu.Unlock()
}

func (f *Faults) FailResponse(operation Operation, err error) {
	f.mu.Lock()
	f.responseFailures[operation] = err
	f.mu.Unlock()
}

// EndWatch delivers a real initial snapshot, then waits for after before failing
// the next receive. A nil signal injects the failure immediately.
func (f *Faults) EndWatch(operation Operation, err error, after <-chan struct{}) {
	f.mu.Lock()
	f.watchFailures[operation] = watchFailure{err: err, after: after}
	f.mu.Unlock()
}

func (f *Faults) Count(operation Operation) int {
	count := 0

	for _, value := range f.Sequence() {
		if value == operation {
			count++
		}
	}

	return count
}

func (f *Faults) Sequence() []Operation {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]Operation(nil), f.sequence...)
}

func (f *Faults) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, gate := range f.allGates {
		gate.Deliver()
	}

	clear(f.failures)
	clear(f.responseFailures)
	clear(f.watchFailures)
}
