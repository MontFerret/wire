package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/wire/internal/lifecycle"
	"github.com/google/uuid"
)

type (
	ExecutionState uint8

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
		Parameters        map[string]any
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
		parameters    map[string]any
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
		parameters:  cloneParameters(input.Parameters),
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
	var session api.Session
	closeAttempted := false
	defer func() {
		if recover() == nil {
			return
		}

		if !isNil(session) && !closeAttempted {
			closeAttempted = true
			_ = closeAPISession(session)
		}

		e.finish(nil, errors.New("runtime execution panicked"), ErrorInternal)
	}()

	options := []api.SessionOption{api.WithParams(cloneParameters(e.parameters))}
	if e.contentType != "" {
		options = append(options, api.WithOutputContentType(e.contentType))
	}

	var err error
	session, err = e.plan.plan.NewSession(e.ctx, options...)
	if err != nil {
		if !isNil(session) {
			closeAttempted = true
			err = errors.Join(err, closeAPISession(session))
			session = nil
		}

		e.finish(nil, err, ErrorInternal)
		return
	}

	if isNil(session) {
		e.finish(nil, errors.New("runtime returned no session"), ErrorInternal)
		return
	}

	output, runErr := session.Run(e.ctx)
	closeAttempted = true
	closeErr := closeAPISession(session)
	session = nil
	err = errors.Join(runErr, closeErr)
	result := &Output{ContentType: output.ContentType, Content: append([]byte(nil), output.Content...)}
	category := ErrorInternal
	if runErr != nil {
		category = ErrorExecution
	}
	e.finish(result, err, category)
}

func closeAPISession(session api.Session) (err error) {
	defer func() {
		if recover() != nil {
			err = internalError(errors.New("runtime session cleanup panicked"))
		}
	}()

	return session.Close()
}

func (e *Execution) finish(output *Output, err error, category ErrorCategory) {
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
		e.failure = failureFromError(category)
		e.publishLocked(ExecutionEventFailed, true)
	default:
		e.state = ExecutionCompleted
		e.publishLocked(ExecutionEventCompleted, true)
	}
}

func cloneParameters(values map[string]any) map[string]any {
	result := make(map[string]any, len(values))
	for name, value := range values {
		result[name] = cloneParameter(value)
	}

	return result
}

func cloneParameter(value any) any {
	switch value := value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = cloneParameter(item)
		}

		return result
	case map[string]any:
		return cloneParameters(value)
	default:
		return value
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
