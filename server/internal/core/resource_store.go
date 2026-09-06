package core

import (
	"context"
	"errors"
	"sync"

	"github.com/MontFerret/wire/server/internal/lifecycle"
)

type (
	// ResourceLimits bounds one logical connection, including pending and closing resources.
	ResourceLimits struct {
		Plans         int
		Sessions      int
		Executions    int
		DebugSessions int
		Watchers      int
		Breakpoints   int
	}

	// ResourceStore owns the resources of exactly one logical connection.
	// mu protects membership, reservations, parent links, and release admission.
	// Resource state locks must never be held when acquiring mu. No hosted call
	// or cleanup wait runs under mu.
	ResourceStore struct {
		mu            sync.Mutex
		ctx           context.Context
		limits        ResourceLimits
		pending       [4]int
		creating      sync.WaitGroup
		closing       bool
		close         lifecycle.Close
		plans         map[PlanID]*Plan
		sessions      map[SessionID]*Session
		executions    map[ExecutionID]*Execution
		debugSessions map[DebugSessionID]*DebugSession
	}

	resourceKind uint8
)

const (
	planResource resourceKind = iota
	sessionResource
	executionResource
	debugResource
)

func newResourceStore(ctx context.Context, limits ResourceLimits) *ResourceStore {
	return &ResourceStore{
		ctx:           ctx,
		limits:        limits,
		plans:         make(map[PlanID]*Plan),
		sessions:      make(map[SessionID]*Session),
		executions:    make(map[ExecutionID]*Execution),
		debugSessions: make(map[DebugSessionID]*DebugSession),
	}
}

// operationError preserves connection-closure precedence for creation requests.
func (r *ResourceStore) operationError(ctx context.Context) error {
	r.mu.Lock()
	err := r.checkOpen(nil)
	r.mu.Unlock()
	if err != nil {
		return err
	}

	return ctx.Err()
}

// beginCreation reserves capacity before external allocation and joins both
// connection and parent creation gates. finishCreation is required on every path.
func (r *ResourceStore) beginCreation(kind resourceKind, plan *Plan) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.checkOpen(plan); err != nil {
		return err
	}

	var count, limit int
	var name string
	switch kind {
	case planResource:
		count, limit, name = len(r.plans), r.limits.Plans, "plan"
	case sessionResource:
		count, limit, name = len(r.sessions), r.limits.Sessions, "session"
	case executionResource:
		count, limit, name = len(r.executions), r.limits.Executions, "execution"
	case debugResource:
		count, limit, name = len(r.debugSessions), r.limits.DebugSessions, "debug session"
	}

	if count+r.pending[kind] >= limit {
		return resourceExhausted(name + " limit reached")
	}

	r.pending[kind]++
	r.creating.Add(1)
	if plan != nil {
		plan.creating.Add(1)
	}

	return nil
}

func (r *ResourceStore) finishCreation(kind resourceKind, plan *Plan, committed bool) {
	if !committed {
		r.mu.Lock()
		r.pending[kind]--
		r.mu.Unlock()
	}

	if plan != nil {
		plan.creating.Done()
	}

	r.creating.Done()
}

// checkOpen requires mu. Publication uses the same gate as release admission.
func (r *ResourceStore) checkOpen(plan *Plan) error {
	if r.closing || r.ctx.Err() != nil {
		return invalidState("connection is closed", context.Canceled)
	}

	if plan != nil && (r.plans[plan.id] != plan || plan.release.Started()) {
		return notFound(ErrorKindPlanNotFound, string(plan.id))
	}

	return nil
}

func (r *ResourceStore) Plan(ctx context.Context, id PlanID) (*Plan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := validateID(id, "plan ID"); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	resource := r.plans[id]
	if resource == nil || resource.release.Started() {
		return nil, notFound(ErrorKindPlanNotFound, string(id))
	}

	return resource, nil
}

func (r *ResourceStore) ReleasePlan(ctx context.Context, id PlanID) error {
	if err := validateID(id, "plan ID"); err != nil {
		return err
	}

	r.mu.Lock()
	resource := r.plans[id]
	r.mu.Unlock()
	if resource == nil {
		return notFound(ErrorKindPlanNotFound, string(id))
	}

	return resource.Release(ctx)
}

func (r *ResourceStore) Session(ctx context.Context, id SessionID) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := validateID(id, "session ID"); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	resource := r.sessions[id]
	if resource == nil || resource.release.Started() {
		return nil, notFound(ErrorKindSessionNotFound, string(id))
	}

	return resource, nil
}

func (r *ResourceStore) ReleaseSession(ctx context.Context, id SessionID) error {
	if err := validateID(id, "session ID"); err != nil {
		return err
	}

	r.mu.Lock()
	resource := r.sessions[id]
	r.mu.Unlock()
	if resource == nil {
		return notFound(ErrorKindSessionNotFound, string(id))
	}

	return resource.Release(ctx)
}

func (r *ResourceStore) Execution(ctx context.Context, id ExecutionID) (*Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := validateID(id, "execution ID"); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	resource := r.executions[id]
	if resource == nil || resource.release.Started() {
		return nil, notFound(ErrorKindExecutionNotFound, string(id))
	}

	return resource, nil
}

func (r *ResourceStore) ReleaseExecution(ctx context.Context, id ExecutionID) error {
	if err := validateID(id, "execution ID"); err != nil {
		return err
	}

	r.mu.Lock()
	resource := r.executions[id]
	r.mu.Unlock()
	if resource == nil {
		return notFound(ErrorKindExecutionNotFound, string(id))
	}

	return resource.Release(ctx)
}

func (r *ResourceStore) DebugSession(ctx context.Context, id DebugSessionID) (*DebugSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := validateID(id, "debug session ID"); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	resource := r.debugSessions[id]
	if resource == nil || resource.release.Started() {
		return nil, notFound(ErrorKindDebugSessionNotFound, string(id))
	}

	return resource, nil
}

func (r *ResourceStore) ReleaseDebugSession(ctx context.Context, id DebugSessionID) error {
	if err := validateID(id, "debug session ID"); err != nil {
		return err
	}

	r.mu.Lock()
	resource := r.debugSessions[id]
	r.mu.Unlock()
	if resource == nil {
		return notFound(ErrorKindDebugSessionNotFound, string(id))
	}

	return resource.Release(ctx)
}

func (r *ResourceStore) Close(ctx context.Context) error {
	r.mu.Lock()
	started := r.close.Begin()
	r.closing = true
	r.mu.Unlock()
	if started {
		go r.settleClose()
	}

	return r.close.Wait(ctx)
}

func (r *ResourceStore) settleClose() {
	var err error
	defer func() {
		if recover() != nil {
			err = errors.Join(err, internalError(errors.New("resource cleanup panicked")))
		}

		r.close.Finish(err)
	}()

	r.creating.Wait()
	r.mu.Lock()
	executions := make([]*Execution, 0, len(r.executions))
	for _, execution := range r.executions {
		executions = append(executions, execution)
	}

	sessions := make([]*Session, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session)
	}

	debugSessions := make([]*DebugSession, 0, len(r.debugSessions))
	for _, session := range r.debugSessions {
		debugSessions = append(debugSessions, session)
	}

	plans := make([]*Plan, 0, len(r.plans))
	for _, plan := range r.plans {
		plans = append(plans, plan)
	}

	r.mu.Unlock()
	for _, execution := range executions {
		err = errors.Join(err, execution.Release(context.Background()))
	}

	for _, session := range sessions {
		err = errors.Join(err, session.Release(context.Background()))
	}

	for _, session := range debugSessions {
		err = errors.Join(err, session.Release(context.Background()))
	}

	for _, plan := range plans {
		err = errors.Join(err, plan.Release(context.Background()))
	}
}

func (r *ResourceStore) registerPlan(ctx context.Context, p *Plan) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := r.checkOpen(nil); err != nil {
		return err
	}

	if r.plans[p.id] != nil {
		return invalidState("plan ID is already registered", nil)
	}

	r.pending[planResource]--
	r.plans[p.id] = p

	return nil
}

func (r *ResourceStore) removePlan(p *Plan) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.plans, p.id)
}

func (r *ResourceStore) registerSession(ctx context.Context, s *Session) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := r.checkOpen(s.plan); err != nil {
		return err
	}

	if r.sessions[s.id] != nil {
		return invalidState("session ID is already registered", nil)
	}

	r.pending[sessionResource]--
	r.sessions[s.id] = s
	s.plan.sessions[s.id] = s

	return nil
}

func (r *ResourceStore) removeSession(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.sessions, s.id)
	delete(s.plan.sessions, s.id)
}

func (r *ResourceStore) registerExecution(ctx context.Context, e *Execution) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := r.checkOpen(e.plan); err != nil {
		return err
	}

	if e.session != nil && (r.sessions[e.session.id] != e.session || e.session.release.Started() || e.session.active != e) {
		return notFound(ErrorKindSessionNotFound, string(e.session.id))
	}

	if r.executions[e.id] != nil {
		return invalidState("execution ID is already registered", nil)
	}

	r.pending[executionResource]--
	r.executions[e.id] = e
	if e.plan != nil {
		e.plan.executions[e.id] = e
	}

	return nil
}

func (r *ResourceStore) removeExecution(e *Execution) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.executions, e.id)
	if e.plan != nil {
		delete(e.plan.executions, e.id)
	}

	if e.session != nil && e.session.active == e {
		e.session.active = nil
	}
}

func (r *ResourceStore) registerDebugSession(ctx context.Context, d *DebugSession) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := r.checkOpen(d.plan); err != nil {
		return err
	}

	if r.debugSessions[d.id] != nil {
		return invalidState("debug session ID is already registered", nil)
	}

	r.pending[debugResource]--
	r.debugSessions[d.id] = d
	d.plan.debugSessions[d.id] = d

	return nil
}

func (r *ResourceStore) removeDebugSession(d *DebugSession) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.debugSessions, d.id)
	delete(d.plan.debugSessions, d.id)
}
