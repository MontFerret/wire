package core

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

// executionStore is the connection-wide index for plan-owned executions. Plan
// membership remains the ownership authority; the index serves ID-only RPCs.
type executionStore struct {
	mu          sync.RWMutex
	connection  *Connection
	max         int
	maxWatchers int
	pending     int
	active      map[ExecutionID]*Execution
	closing     map[ExecutionID]*Execution
}

func newExecutionStore(connection *Connection, maxExecutions, maxWatchers int) *executionStore {
	return &executionStore{
		connection:  connection,
		max:         maxExecutions,
		maxWatchers: maxWatchers,
		active:      make(map[ExecutionID]*Execution),
		closing:     make(map[ExecutionID]*Execution),
	}
}

func (c *Connection) Execute(ctx context.Context, input ExecuteInput) (ExecutionSnapshot, error) {
	return c.executions.create(ctx, input)
}

func (c *Connection) CancelExecution(id ExecutionID) (ExecutionSnapshot, error) {
	return c.executions.cancel(id)
}

func (c *Connection) WatchExecution(id ExecutionID) (ExecutionSubscription, error) {
	return c.executions.watch(id)
}

func (c *Connection) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	return c.executions.release(ctx, id)
}

func (s *executionStore) create(ctx context.Context, input ExecuteInput) (ExecutionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionSnapshot{}, err
	}

	if err := validateID(input.PlanID, "plan ID"); err != nil {
		return ExecutionSnapshot{}, err
	}

	if err := s.connection.beginOperation(); err != nil {
		return ExecutionSnapshot{}, err
	}
	defer s.connection.finishOperation()

	if err := s.reserveCreation(); err != nil {
		return ExecutionSnapshot{}, err
	}
	committed := false
	defer func() {
		if !committed {
			s.rollbackCreation()
		}
	}()

	var created *Execution
	err := s.connection.commitCreation(func() error {
		return s.connection.plans.withActive(input.PlanID, nil, func(plan *Plan) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			executionCtx, cancel := context.WithCancelCause(s.connection.ctx)
			created = &Execution{
				id:          ExecutionID(uuid.NewString()),
				plan:        plan,
				ctx:         executionCtx,
				cancel:      cancel,
				parameters:  cloneParameters(input.Parameters),
				contentType: input.OutputContentType,
				maxWatchers: s.maxWatchers,
				state:       ExecutionRunning,
				watchers:    make(map[uint64]*executionWatcher),
				done:        make(chan struct{}),
			}
			created.publishLocked(ExecutionEventStarted, false)

			s.mu.Lock()
			s.pending--
			s.active[created.id] = created
			plan.executions[created.id] = struct{}{}
			s.mu.Unlock()
			committed = true

			return nil
		})
	})
	if err != nil {
		return ExecutionSnapshot{}, err
	}

	go created.run()

	return created.snapshot(), nil
}

func (s *executionStore) cancel(id ExecutionID) (ExecutionSnapshot, error) {
	execution, err := s.lookup(id)
	if err != nil {
		return ExecutionSnapshot{}, err
	}

	execution.cancel(context.Canceled)

	return execution.snapshot(), nil
}

func (s *executionStore) watch(id ExecutionID) (ExecutionSubscription, error) {
	execution, err := s.lookup(id)
	if err != nil {
		return ExecutionSubscription{}, err
	}

	return execution.subscribe()
}

func (s *executionStore) release(ctx context.Context, id ExecutionID) error {
	if err := validateID(id, "execution ID"); err != nil {
		return err
	}

	s.mu.Lock()
	execution := s.active[id]
	if execution != nil {
		delete(s.active, id)
		s.closing[id] = execution
	} else {
		execution = s.closing[id]
	}
	s.mu.Unlock()

	if execution == nil {
		return notFound(ErrorExecutionNotFound, string(id))
	}

	if execution.close.Begin() {
		go s.settleRelease(execution)
	}

	return execution.close.Wait(ctx)
}

func (s *executionStore) settleRelease(execution *Execution) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("execution cleanup panicked")))
		}

		execution.plan.mu.Lock()
		delete(execution.plan.executions, execution.id)
		execution.plan.mu.Unlock()

		s.mu.Lock()
		if s.closing[execution.id] == execution {
			delete(s.closing, execution.id)
		}
		s.mu.Unlock()

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

func (s *executionStore) lookup(id ExecutionID) (*Execution, error) {
	if err := validateID(id, "execution ID"); err != nil {
		return nil, err
	}

	s.mu.RLock()
	execution := s.active[id]
	s.mu.RUnlock()

	if execution == nil {
		return nil, notFound(ErrorExecutionNotFound, string(id))
	}

	return execution, nil
}

func (s *executionStore) reserveCreation() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending+len(s.active)+len(s.closing) >= s.max {
		return resourceExhausted("execution limit reached")
	}

	s.pending++

	return nil
}

func (s *executionStore) rollbackCreation() {
	s.mu.Lock()
	s.pending--
	s.mu.Unlock()
}

func (s *executionStore) closeAll() error {
	s.mu.RLock()
	ids := make([]ExecutionID, 0, len(s.active)+len(s.closing))
	for id := range s.active {
		ids = append(ids, id)
	}

	for id := range s.closing {
		if s.active[id] == nil {
			ids = append(ids, id)
		}
	}
	s.mu.RUnlock()

	var result error
	for _, id := range ids {
		err := s.release(context.Background(), id)
		result = errors.Join(result, ignoreMissingResource(err, ErrorExecutionNotFound))
	}

	return result
}
