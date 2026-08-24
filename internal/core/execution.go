package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/ferret/v2"
	ferretruntime "github.com/MontFerret/ferret/v2/pkg/runtime"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

type (
	ExecutionState     uint8
	ExecutionEventKind uint8

	Output struct {
		ContentType string
		Content     []byte
	}

	Failure struct {
		Category    ErrorCategory
		Message     string
		Diagnostics []Diagnostic
	}

	ExecutionSnapshot struct {
		ID      ExecutionID
		PlanID  PlanID
		State   ExecutionState
		Output  *Output
		Failure *Failure
	}

	ExecutionEvent struct {
		Execution ExecutionID
		Sequence  uint64
		Kind      ExecutionEventKind
		Snapshot  ExecutionSnapshot
	}

	ExecutionSubscription struct {
		Current ExecutionEvent
		Events  <-chan ExecutionEvent
		Errors  <-chan error
		Cancel  func()
	}

	ExecuteInput struct {
		PlanID            PlanID
		Parameters        ferretruntime.Params
		OutputContentType string
	}

	executionWatcher struct {
		events chan ExecutionEvent
		errors chan error
	}

	Execution struct {
		mu            sync.Mutex
		id            ExecutionID
		plan          *Plan
		ctx           context.Context
		cancel        context.CancelCauseFunc
		parameters    ferretruntime.Params
		contentType   string
		maxWatchers   int
		state         ExecutionState
		output        *Output
		failure       *Failure
		sequence      uint64
		lastEvent     ExecutionEvent
		nextWatcher   uint64
		subscriptions int
		watchers      map[uint64]*executionWatcher
		done          chan struct{}
		close         lifecycle.Close
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
	ExecutionEventCompleted
	ExecutionEventFailed
	ExecutionEventCancelled
)

const watcherBufferSize = 8

func (c *Connection) Execute(ctx context.Context, input ExecuteInput) (ExecutionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return ExecutionSnapshot{}, err
	}

	c.mu.Lock()
	if err := c.ensureOpenLocked(); err != nil {
		c.mu.Unlock()
		return ExecutionSnapshot{}, err
	}

	if len(c.executions)+len(c.closingExecutions) >= c.limits.MaxExecutionsPerConnection {
		c.mu.Unlock()
		return ExecutionSnapshot{}, resourceExhausted("execution limit reached")
	}
	plan := c.plans[input.PlanID]
	if plan == nil {
		c.mu.Unlock()
		return ExecutionSnapshot{}, notFound(ErrorPlanNotFound, string(input.PlanID))
	}
	plan.mu.Lock()
	if plan.closing {
		plan.mu.Unlock()
		c.mu.Unlock()
		return ExecutionSnapshot{}, notFound(ErrorPlanNotFound, string(input.PlanID))
	}

	if err := ctx.Err(); err != nil {
		plan.mu.Unlock()
		c.mu.Unlock()
		return ExecutionSnapshot{}, err
	}

	executionCtx, cancel := context.WithCancelCause(c.ctx)
	execution := &Execution{
		id:          ExecutionID(uuid.NewString()),
		plan:        plan,
		ctx:         executionCtx,
		cancel:      cancel,
		parameters:  input.Parameters.Clone(),
		contentType: input.OutputContentType,
		maxWatchers: c.limits.MaxWatchersPerResource,
		state:       ExecutionRunning,
		watchers:    make(map[uint64]*executionWatcher),
		done:        make(chan struct{}),
	}
	execution.publishLocked(ExecutionEventStarted, false)
	plan.executions[execution.id] = struct{}{}
	plan.mu.Unlock()
	c.executions[execution.id] = execution
	c.mu.Unlock()

	go execution.run()

	return execution.snapshot(), nil
}

func (e *Execution) run() {
	var session *ferret.Session
	defer func() {
		if recover() == nil {
			return
		}

		if session != nil {
			func() {
				defer func() { _ = recover() }()
				_ = session.Close()
			}()
		}

		e.finish(nil, errors.New("Ferret execution panicked"))
	}()

	options := []ferret.SessionOption{ferret.WithSessionRuntimeParams(e.parameters)}
	if e.contentType != "" {
		options = append(options, ferret.WithOutputContentType(e.contentType))
	}

	var err error
	session, err = e.plan.plan.NewSession(e.ctx, options...)
	if err != nil {
		e.finish(nil, err)
		return
	}

	output, runErr := session.Run(e.ctx)
	closeErr := session.Close()
	session = nil
	err = errors.Join(runErr, closeErr)
	var result *Output
	if output != nil {
		result = &Output{ContentType: output.ContentType, Content: append([]byte(nil), output.Content...)}
	}
	e.finish(result, err)
}

func (e *Execution) finish(output *Output, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != ExecutionRunning {
		return
	}

	e.output = output
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(context.Cause(e.ctx), context.Canceled):
		e.state = ExecutionCancelled
		e.publishLocked(ExecutionEventCancelled, true)
	case err != nil:
		e.state = ExecutionFailed
		e.failure = failureFromError(err, e.plan.identity)
		e.publishLocked(ExecutionEventFailed, true)
	default:
		e.state = ExecutionCompleted
		e.publishLocked(ExecutionEventCompleted, true)
	}
}

func (e *Execution) snapshot() ExecutionSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.snapshotLocked()
}

func (e *Execution) snapshotLocked() ExecutionSnapshot {
	result := ExecutionSnapshot{ID: e.id, PlanID: e.plan.id, State: e.state}
	if e.output != nil {
		result.Output = &Output{ContentType: e.output.ContentType, Content: append([]byte(nil), e.output.Content...)}
	}

	if e.failure != nil {
		result.Failure = &Failure{
			Category:    e.failure.Category,
			Message:     e.failure.Message,
			Diagnostics: cloneDiagnostics(e.failure.Diagnostics),
		}
	}

	return result
}

func cloneDiagnostics(values []Diagnostic) []Diagnostic {
	result := make([]Diagnostic, len(values))
	for i, value := range values {
		result[i] = value
		result[i].Spans = append([]DiagnosticSpan(nil), value.Spans...)
	}

	return result
}

func (c *Connection) execution(id ExecutionID) (*Execution, error) {
	if err := validateID(id, "execution ID"); err != nil {
		return nil, err
	}
	c.mu.RLock()
	execution := c.executions[id]
	c.mu.RUnlock()
	if execution == nil {
		return nil, notFound(ErrorExecutionNotFound, string(id))
	}

	return execution, nil
}

func (c *Connection) CancelExecution(id ExecutionID) (ExecutionSnapshot, error) {
	execution, err := c.execution(id)
	if err != nil {
		return ExecutionSnapshot{}, err
	}
	execution.cancel(context.Canceled)

	return execution.snapshot(), nil
}

func (c *Connection) WatchExecution(id ExecutionID) (ExecutionSubscription, error) {
	execution, err := c.execution(id)
	if err != nil {
		return ExecutionSubscription{}, err
	}

	return execution.subscribe()
}

func (e *Execution) subscribe() (ExecutionSubscription, error) {
	e.mu.Lock()
	if e.subscriptions >= e.maxWatchers {
		e.mu.Unlock()
		return ExecutionSubscription{}, resourceExhausted("execution watcher limit reached")
	}
	e.subscriptions++
	e.nextWatcher++
	id := e.nextWatcher
	current := e.lastEvent.clone()
	if e.state != ExecutionRunning {
		events := make(chan ExecutionEvent)
		errorsChannel := make(chan error)
		close(events)
		close(errorsChannel)
		e.mu.Unlock()
		var once sync.Once
		return ExecutionSubscription{Current: current, Events: events, Errors: errorsChannel, Cancel: func() {
			once.Do(func() { e.unsubscribe(id) })
		}}, nil
	}
	watcher := &executionWatcher{events: make(chan ExecutionEvent, watcherBufferSize), errors: make(chan error, 1)}
	e.watchers[id] = watcher
	e.mu.Unlock()

	var once sync.Once
	return ExecutionSubscription{
		Current: current,
		Events:  watcher.events,
		Errors:  watcher.errors,
		Cancel: func() {
			once.Do(func() { e.unsubscribe(id) })
		},
	}, nil
}

func (e *Execution) publishLocked(kind ExecutionEventKind, terminal bool) {
	e.sequence++
	e.lastEvent = ExecutionEvent{Execution: e.id, Sequence: e.sequence, Kind: kind, Snapshot: e.snapshotLocked()}
	for id, watcher := range e.watchers {
		select {
		case watcher.events <- e.lastEvent.clone():
			if terminal {
				e.closeWatcherLocked(id, watcher, nil)
			}
		default:
			e.closeWatcherLocked(id, watcher, ErrWatcherLagged)
		}
	}

	if terminal {
		close(e.done)
	}
}

func (e ExecutionEvent) clone() ExecutionEvent {
	e.Snapshot = e.Snapshot.clone()
	return e
}

func (s ExecutionSnapshot) clone() ExecutionSnapshot {
	result := s
	if s.Output != nil {
		result.Output = &Output{ContentType: s.Output.ContentType, Content: append([]byte(nil), s.Output.Content...)}
	}

	if s.Failure != nil {
		result.Failure = &Failure{Category: s.Failure.Category, Message: s.Failure.Message, Diagnostics: cloneDiagnostics(s.Failure.Diagnostics)}
	}

	return result
}

func (e *Execution) unsubscribe(id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if watcher := e.watchers[id]; watcher != nil {
		e.closeWatcherLocked(id, watcher, nil)
	}

	if e.subscriptions > 0 {
		e.subscriptions--
	}
}

func (e *Execution) closeWatcherLocked(id uint64, watcher *executionWatcher, err error) {
	if err != nil {
		watcher.errors <- err
	}
	close(watcher.events)
	close(watcher.errors)
	delete(e.watchers, id)
}

func (c *Connection) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	if err := validateID(id, "execution ID"); err != nil {
		return err
	}
	c.mu.Lock()
	execution := c.executions[id]
	if execution != nil {
		delete(c.executions, id)
		c.closingExecutions[id] = execution
	} else {
		execution = c.closingExecutions[id]
	}

	if execution != nil {
		execution.plan.mu.Lock()
		delete(execution.plan.executions, id)
		execution.plan.mu.Unlock()
	}
	c.mu.Unlock()
	if execution == nil {
		return notFound(ErrorExecutionNotFound, string(id))
	}

	if execution.close.Begin() {
		go c.settleExecutionRelease(execution)
	}

	return execution.close.Wait(ctx)
}

func (c *Connection) settleExecutionRelease(execution *Execution) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("execution cleanup panicked")))
		}

		c.mu.Lock()
		if c.closingExecutions[execution.id] == execution {
			delete(c.closingExecutions, execution.id)
		}
		c.mu.Unlock()
		execution.close.Finish(err)
	}()

	execution.cancel(context.Canceled)
	<-execution.done
	execution.mu.Lock()
	for id, watcher := range execution.watchers {
		execution.closeWatcherLocked(id, watcher, nil)
	}
	execution.mu.Unlock()
}
