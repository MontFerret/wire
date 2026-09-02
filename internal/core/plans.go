package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/api"
	"github.com/google/uuid"
)

// planStore owns the connection's plan index, capacity, and release lifecycle.
type planStore struct {
	mu            sync.RWMutex
	connection    *Connection
	runtime       api.Runtime
	max           int
	pending       int
	active        map[PlanID]*Plan
	closing       map[PlanID]*Plan
	executions    *executionStore
	debugSessions *debugSessionStore
}

func newPlanStore(connection *Connection, runtime api.Runtime, maxPlans int) *planStore {
	return &planStore{
		connection: connection,
		runtime:    runtime,
		max:        maxPlans,
		active:     make(map[PlanID]*Plan),
		closing:    make(map[PlanID]*Plan),
	}
}

func (s *planStore) attachChildren(executions *executionStore, debugSessions *debugSessionStore) {
	s.executions = executions
	s.debugSessions = debugSessions
}

func (c *Connection) Compile(ctx context.Context, input CompileInput) (PlanSnapshot, error) {
	return c.plans.compile(ctx, input)
}

func (c *Connection) ReleasePlan(ctx context.Context, id PlanID) error {
	return c.plans.release(ctx, id)
}

func (s *planStore) compile(ctx context.Context, input CompileInput) (PlanSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return PlanSnapshot{}, err
	}

	if input.Source.Content == "" {
		return PlanSnapshot{}, invalidRequest("source content is required")
	}

	if input.Source.Name == "" {
		input.Source.Name = "anonymous"
	}

	if err := s.connection.beginOperation(); err != nil {
		return PlanSnapshot{}, err
	}
	defer s.connection.finishOperation()

	if err := s.reserveCreation(); err != nil {
		return PlanSnapshot{}, err
	}
	defer s.finishCreation()

	compileCtx, cancel := s.connection.operationContext(ctx)
	defer cancel()

	compiled, err, panicked := s.compileAPIPlan(compileCtx, input)
	if panicked {
		return PlanSnapshot{}, internalError(err)
	}

	if err != nil {
		compileErr := &DomainError{
			Category: ErrorCompilation,
			Message:  "compilation failed",
			Cause:    err,
		}
		if !isNil(compiled) {
			return PlanSnapshot{}, errors.Join(compileErr, closeAPIPlan(compiled))
		}

		return PlanSnapshot{}, compileErr
	}

	if isNil(compiled) {
		return PlanSnapshot{}, internalError(errors.New("runtime returned no plan"))
	}

	if err := compileCtx.Err(); err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	parameters, err := apiPlanParameters(compiled)
	if err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	created := &Plan{
		id:            PlanID(uuid.NewString()),
		plan:          compiled,
		parameters:    parameters,
		debuggable:    input.Debuggable,
		executions:    make(map[ExecutionID]struct{}),
		debugSessions: make(map[DebugSessionID]struct{}),
	}

	err = s.connection.commitCreation(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		s.mu.Lock()
		s.active[created.id] = created
		s.mu.Unlock()

		return nil
	})
	if err != nil {
		return PlanSnapshot{}, errors.Join(err, closeAPIPlan(compiled))
	}

	return created.snapshot(), nil
}

func (s *planStore) release(ctx context.Context, id PlanID) error {
	if err := validateID(id, "plan ID"); err != nil {
		return err
	}

	s.mu.Lock()
	plan := s.active[id]
	if plan == nil {
		plan = s.closing[id]
	}

	if plan == nil {
		s.mu.Unlock()

		return notFound(ErrorPlanNotFound, string(id))
	}

	if s.active[id] == plan {
		plan.mu.Lock()
		plan.closing = true
		plan.mu.Unlock()
		delete(s.active, id)
		s.closing[id] = plan
	}

	s.mu.Unlock()

	if plan.release.Begin() {
		go s.settleRelease(plan)
	}

	return plan.release.Wait(ctx)
}

func (s *planStore) settleRelease(plan *Plan) {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("plan cleanup panicked")))
		}

		s.mu.Lock()
		if s.closing[plan.id] == plan {
			delete(s.closing, plan.id)
		}
		s.mu.Unlock()

		plan.release.Finish(err)
	}()

	plan.debugCreations.Wait()

	plan.mu.Lock()
	executionIDs := make([]ExecutionID, 0, len(plan.executions))
	for id := range plan.executions {
		executionIDs = append(executionIDs, id)
	}

	debugIDs := make([]DebugSessionID, 0, len(plan.debugSessions))
	for id := range plan.debugSessions {
		debugIDs = append(debugIDs, id)
	}
	plan.mu.Unlock()

	for _, id := range debugIDs {
		releaseErr := s.debugSessions.release(context.Background(), id)
		err = errors.Join(err, ignoreMissingResource(releaseErr, ErrorDebugSessionNotFound))
	}

	for _, id := range executionIDs {
		releaseErr := s.executions.release(context.Background(), id)
		err = errors.Join(err, ignoreMissingResource(releaseErr, ErrorExecutionNotFound))
	}

	err = errors.Join(err, closeAPIPlan(plan.plan))
}

func (s *planStore) lookup(id PlanID) (*Plan, error) {
	if err := validateID(id, "plan ID"); err != nil {
		return nil, err
	}

	s.mu.RLock()
	plan := s.active[id]
	s.mu.RUnlock()

	if plan == nil {
		return nil, notFound(ErrorPlanNotFound, string(id))
	}

	return plan, nil
}

// withActive locks the plan store before the plan so child publication is
// atomic with respect to plan release. The callback must not call external code.
func (s *planStore) withActive(id PlanID, expected *Plan, operation func(*Plan) error) error {
	s.mu.RLock()
	plan := s.active[id]
	if plan == nil || expected != nil && plan != expected {
		s.mu.RUnlock()

		return notFound(ErrorPlanNotFound, string(id))
	}

	plan.mu.Lock()
	if plan.closing {
		plan.mu.Unlock()
		s.mu.RUnlock()

		return notFound(ErrorPlanNotFound, string(id))
	}

	err := operation(plan)
	plan.mu.Unlock()
	s.mu.RUnlock()

	return err
}

// beginDebugCreation prevents api.Plan.Close from racing a runtime debugger
// constructor while allowing the constructor itself to run without locks held.
func (s *planStore) beginDebugCreation(id PlanID) (*Plan, error) {
	s.mu.RLock()
	plan := s.active[id]
	if plan == nil {
		s.mu.RUnlock()

		return nil, notFound(ErrorPlanNotFound, string(id))
	}

	plan.mu.Lock()
	defer plan.mu.Unlock()
	defer s.mu.RUnlock()

	if plan.closing {
		return nil, notFound(ErrorPlanNotFound, string(id))
	}

	if !plan.debuggable {
		return nil, invalidState("plan was not compiled for debugging", nil)
	}

	plan.debugCreations.Add(1)

	return plan, nil
}

func (s *planStore) reserveCreation() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending+len(s.active)+len(s.closing) >= s.max {
		return resourceExhausted("plan limit reached")
	}

	s.pending++

	return nil
}

func (s *planStore) finishCreation() {
	s.mu.Lock()
	s.pending--
	s.mu.Unlock()
}

func (s *planStore) closeAll() error {
	s.mu.RLock()
	ids := make([]PlanID, 0, len(s.active)+len(s.closing))
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
		result = errors.Join(result, ignoreMissingResource(err, ErrorPlanNotFound))
	}

	return result
}

func (s *planStore) compileAPIPlan(ctx context.Context, input CompileInput) (compiled api.Plan, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			compiled = nil
			err = errors.New("runtime compilation panicked")
			panicked = true
		}
	}()

	var options []api.PlanOption
	if input.OptimizationLevel != nil {
		options = append(options, api.WithOptimizationLevel(*input.OptimizationLevel))
	}

	if input.Debuggable {
		compiled, err = s.runtime.CompileDebug(ctx, input.Source, options...)
	} else {
		compiled, err = s.runtime.Compile(ctx, input.Source, options...)
	}

	return
}
