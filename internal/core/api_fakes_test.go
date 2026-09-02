package core

import (
	"context"
	"sync"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/source"
)

type (
	spyRuntime struct {
		mu                  sync.Mutex
		compile             func(context.Context, api.Source, bool) (api.Plan, error)
		compileSources      []api.Source
		compileDebug        []bool
		compileOptimization []*api.OptimizationLevel
		closeCalls          int
	}

	spyPlan struct {
		mu              sync.Mutex
		params          []string
		paramsCall      func() []string
		newSession      func(context.Context, sessionOptions) (api.Session, error)
		newDebugSession func(context.Context, sessionOptions) (debugger.Session, error)
		sessionOptions  []sessionOptions
		debugOptions    []sessionOptions
		close           func() error
		closeCalls      int
	}

	spySession struct {
		mu         sync.Mutex
		run        func(context.Context) (api.Output, error)
		close      func() error
		runCalls   int
		closeCalls int
	}

	spyDebugger struct {
		mu               sync.Mutex
		start            func(context.Context) (*debugger.Event, error)
		resume           func(context.Context) (*debugger.Event, error)
		pause            func() error
		setBreakpoint    func(source.Location, debugger.BreakpointOptions) (debugger.Breakpoint, error)
		deleteBreakpoint func(debugger.BreakpointID) error
		breakpoints      map[debugger.BreakpointID]debugger.Breakpoint
		frames           []debugger.Frame
		locals           []debugger.Variable
		setCalls         int
		deleteCalls      int
		pauseCalls       int
		close            func() error
		closeCalls       int
	}

	sessionOptions struct {
		params      map[string]any
		contentType string
	}

	planOptions struct {
		optimizationLevel *api.OptimizationLevel
	}
)

func (r *spyRuntime) Run(context.Context, api.Source, ...api.SessionOption) (api.Output, error) {
	return api.Output{}, nil
}

func (r *spyRuntime) Compile(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, false, options)
}

func (r *spyRuntime) CompileDebug(ctx context.Context, src api.Source, options ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, true, options)
}

func (r *spyRuntime) compilePlan(ctx context.Context, src api.Source, debug bool, options []api.PlanOption) (api.Plan, error) {
	configured := &planOptions{}
	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(configured); err != nil {
			return nil, err
		}
	}

	r.mu.Lock()
	r.compileSources = append(r.compileSources, src)
	r.compileDebug = append(r.compileDebug, debug)
	r.compileOptimization = append(r.compileOptimization, configured.optimizationLevel)
	compile := r.compile
	r.mu.Unlock()

	if compile == nil {
		return &spyPlan{}, nil
	}

	return compile(ctx, src, debug)
}

func (r *spyRuntime) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()

	return nil
}

func (r *spyRuntime) snapshot() ([]api.Source, []bool, int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]api.Source(nil), r.compileSources...), append([]bool(nil), r.compileDebug...), r.closeCalls
}

func (r *spyRuntime) optimizationSnapshot() []*api.OptimizationLevel {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]*api.OptimizationLevel, len(r.compileOptimization))
	for index, level := range r.compileOptimization {
		if level == nil {
			continue
		}

		copied := *level
		result[index] = &copied
	}

	return result
}

func (options *planOptions) SetOptimizationLevel(level api.OptimizationLevel) error {
	options.optimizationLevel = &level

	return nil
}

func (p *spyPlan) Params() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paramsCall != nil {
		return p.paramsCall()
	}

	return p.params
}

func (p *spyPlan) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	configured, err := applySessionOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.sessionOptions = append(p.sessionOptions, configured.clone())
	newSession := p.newSession
	p.mu.Unlock()

	if newSession == nil {
		return &spySession{}, nil
	}

	return newSession(ctx, configured)
}

func (p *spyPlan) NewDebugSession(ctx context.Context, options ...api.SessionOption) (debugger.Session, error) {
	configured, err := applySessionOptions(options)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.debugOptions = append(p.debugOptions, configured.clone())
	newDebugSession := p.newDebugSession
	p.mu.Unlock()

	if newDebugSession == nil {
		return &spyDebugger{}, nil
	}

	return newDebugSession(ctx, configured)
}

func (p *spyPlan) Close() error {
	p.mu.Lock()
	p.closeCalls++
	closePlan := p.close
	p.mu.Unlock()

	if closePlan == nil {
		return nil
	}

	return closePlan()
}

func (p *spyPlan) snapshot() ([]sessionOptions, []sessionOptions, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sessions := make([]sessionOptions, len(p.sessionOptions))
	for i, options := range p.sessionOptions {
		sessions[i] = options.clone()
	}
	debugSessions := make([]sessionOptions, len(p.debugOptions))
	for i, options := range p.debugOptions {
		debugSessions[i] = options.clone()
	}

	return sessions, debugSessions, p.closeCalls
}

func (s *spySession) Run(ctx context.Context) (api.Output, error) {
	s.mu.Lock()
	s.runCalls++
	run := s.run
	s.mu.Unlock()

	if run == nil {
		return api.Output{}, nil
	}

	return run(ctx)
}

func (s *spySession) Close() error {
	s.mu.Lock()
	s.closeCalls++
	closeSession := s.close
	s.mu.Unlock()

	if closeSession == nil {
		return nil
	}

	return closeSession()
}

func (s *spySession) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.runCalls, s.closeCalls
}

func (d *spyDebugger) Start(ctx context.Context) (*debugger.Event, error) {
	if d.start == nil {
		return &debugger.Event{Reason: debugger.ReasonEntry}, nil
	}

	return d.start(ctx)
}

func (d *spyDebugger) Continue(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) Step(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) Next(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) Out(ctx context.Context) (*debugger.Event, error) {
	return d.resumeDebug(ctx)
}

func (d *spyDebugger) resumeDebug(ctx context.Context) (*debugger.Event, error) {
	if d.resume == nil {
		return &debugger.Event{Reason: debugger.ReasonCompleted}, nil
	}

	return d.resume(ctx)
}

func (d *spyDebugger) Pause() error {
	d.mu.Lock()
	d.pauseCalls++
	pause := d.pause
	d.mu.Unlock()

	if pause == nil {
		return nil
	}

	return pause()
}

func (d *spyDebugger) SetBreakpoint(position source.Location) (debugger.Breakpoint, error) {
	return d.SetBreakpointAt(position, debugger.BreakpointOptions{})
}

func (d *spyDebugger) SetBreakpointAt(position source.Location, options debugger.BreakpointOptions) (debugger.Breakpoint, error) {
	d.mu.Lock()
	d.setCalls++
	setBreakpoint := d.setBreakpoint
	d.mu.Unlock()

	if setBreakpoint != nil {
		return setBreakpoint(position, options)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.breakpoints == nil {
		d.breakpoints = make(map[debugger.BreakpointID]debugger.Breakpoint)
	}

	id := debugger.BreakpointID(len(d.breakpoints) + 1)
	value := debugger.Breakpoint{
		Location: source.Range{
			Location: position,
			Span:     source.Span{Start: 0, End: 1},
		},
		RequestedLocation: position,
		ID:                id,
		PointID:           41,
		FunctionID:        42,
		BindingMode:       options.BindingMode,
		Bound:             true,
	}
	d.breakpoints[id] = value

	return value, nil
}

func (d *spyDebugger) DeleteBreakpoint(id debugger.BreakpointID) error {
	d.mu.Lock()
	d.deleteCalls++
	deleteBreakpoint := d.deleteBreakpoint
	d.mu.Unlock()

	if deleteBreakpoint != nil {
		return deleteBreakpoint(id)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.breakpoints, id)

	return nil
}

func (d *spyDebugger) Breakpoints() []debugger.Breakpoint {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make([]debugger.Breakpoint, 0, len(d.breakpoints))
	for _, value := range d.breakpoints {
		result = append(result, value)
	}

	return result
}

func (d *spyDebugger) Frames() ([]debugger.Frame, error) {
	return append([]debugger.Frame(nil), d.frames...), nil
}

func (d *spyDebugger) Locals() ([]debugger.Variable, error) {
	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) FrameLocals(int) ([]debugger.Variable, error) {
	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) Variables(debugger.ValueReference) ([]debugger.Variable, error) {
	return append([]debugger.Variable(nil), d.locals...), nil
}

func (d *spyDebugger) Evaluate(context.Context, string) (debugger.Value, error) {
	return debugger.Value{Type: "string", Display: "wire"}, nil
}

func (d *spyDebugger) EvaluateFrame(context.Context, int, string) (debugger.Value, error) {
	return debugger.Value{Type: "string", Display: "wire"}, nil
}

func (d *spyDebugger) Close() error {
	d.mu.Lock()
	d.closeCalls++
	closeDebugger := d.close
	d.mu.Unlock()

	if closeDebugger == nil {
		return nil
	}

	return closeDebugger()
}

func (d *spyDebugger) closes() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.closeCalls
}

func (d *spyDebugger) breakpointCalls() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.setCalls, d.deleteCalls
}

func (d *spyDebugger) pauses() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.pauseCalls
}

func (o *sessionOptions) SetParam(name string, value any) error {
	if o.params == nil {
		o.params = make(map[string]any)
	}
	o.params[name] = cloneParameter(value)

	return nil
}

func (o *sessionOptions) SetParams(values map[string]any) error {
	if o.params == nil {
		o.params = make(map[string]any)
	}
	for name, value := range values {
		o.params[name] = cloneParameter(value)
	}

	return nil
}

func (o *sessionOptions) SetOutputContentType(contentType string) error {
	o.contentType = contentType

	return nil
}

func (o sessionOptions) clone() sessionOptions {
	o.params = cloneParameters(o.params)

	return o
}

func applySessionOptions(options []api.SessionOption) (sessionOptions, error) {
	configured := sessionOptions{params: make(map[string]any)}
	for _, option := range options {
		if err := option(&configured); err != nil {
			return sessionOptions{}, err
		}
	}

	return configured, nil
}
