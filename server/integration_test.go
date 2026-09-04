package server_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"github.com/MontFerret/api/debugger"
	"github.com/MontFerret/api/diagnostics"
	"github.com/MontFerret/api/source"
	"github.com/MontFerret/wire/client"
	wirev1 "github.com/MontFerret/wire/gen/ferret/wire/v1"
	"github.com/MontFerret/wire/pkg/failure"
	wireruntime "github.com/MontFerret/wire/pkg/runtime"
	"github.com/MontFerret/wire/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type (
	apiRuntimeSpy struct {
		mu         sync.Mutex
		compile    func(context.Context, api.Source, bool) (api.Plan, error)
		sources    []api.Source
		debug      []bool
		closeCalls int
	}

	apiPlanSpy struct {
		mu             sync.Mutex
		params         []string
		newSession     func(context.Context, apiSessionOptions) (api.Session, error)
		sessionOptions []apiSessionOptions
		closeCalls     int
	}

	apiSessionSpy struct {
		mu         sync.Mutex
		run        func(context.Context) (api.Output, error)
		close      func() error
		closeCalls int
	}

	apiSessionOptions struct {
		params      map[string]any
		contentType string
	}

	integrationEnv struct {
		server   *server.Server
		listener *bufconn.Listener
		conn     *grpc.ClientConn
		client   *client.Client
		serveErr chan error
		shutdown bool
	}
)

func TestUnifiedRuntimeCompileExecuteAndBorrowedOwnership(t *testing.T) {
	outputBytes := []byte(`{"runtime":"unified"}`)
	plan := &apiPlanSpy{
		params: []string{"input"},
		newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
			return &apiSessionSpy{run: func(context.Context) (api.Output, error) {
				return api.Output{ContentType: "application/json", Content: outputBytes}, nil
			}}, nil
		},
	}
	runtime := &apiRuntimeSpy{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}
	env := newIntegrationEnv(t, runtime, server.WithRuntimeIdentity(wireruntime.Identity{
		Name: "test-host", Version: "1.2.3", InstanceID: "instance-1",
	}))

	info := env.client.RuntimeInfo()
	if info.APIIdentity != "ferret.wire" || info.WireVersion != "v1" || info.FerretVersion != "" {
		t.Fatalf("unexpected generic runtime info: %#v", info)
	}
	if info.RuntimeIdentity == nil || info.RuntimeIdentity.Name != "test-host" || info.Capabilities != (client.Capabilities{}) {
		t.Fatalf("unexpected identity or legacy capabilities: %#v", info)
	}

	compiled, err := env.client.Compile(context.Background(), api.Source{
		Name:    "unified.fql",
		Content: "RETURN @input",
	}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(compiled.Parameters(), []string{"input"}) {
		t.Fatalf("unexpected plan parameters: %#v", compiled.Parameters())
	}

	parameters := client.Parameters{
		"input": map[string]any{
			"none":    nil,
			"boolean": true,
			"integer": int64(7),
			"float":   3.5,
			"string":  "wire",
			"binary":  []byte{1, 2},
			"array":   []any{"one", int64(2)},
		},
	}
	for range 2 {
		execution, err := compiled.Execute(context.Background(), parameters, client.ExecuteOptions{OutputContentType: "application/json"})
		if err != nil {
			t.Fatal(err)
		}
		output, err := execution.Wait(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if output.ContentType != "application/json" || string(output.Content) != `{"runtime":"unified"}` {
			t.Fatalf("unexpected output: %#v", output)
		}
		if err := execution.Close(testContext(t)); err != nil {
			t.Fatal(err)
		}
	}

	runtime.mu.Lock()
	sources := append([]api.Source(nil), runtime.sources...)
	debug := append([]bool(nil), runtime.debug...)
	runtime.mu.Unlock()
	if len(sources) != 1 || sources[0] != (api.Source{Name: "unified.fql", Content: "RETURN @input"}) || debug[0] {
		t.Fatalf("unexpected compile delegation: %#v %#v", sources, debug)
	}
	plan.mu.Lock()
	options := append([]apiSessionOptions(nil), plan.sessionOptions...)
	plan.mu.Unlock()
	if len(options) != 2 || options[0].contentType != "application/json" || options[1].contentType != "application/json" {
		t.Fatalf("plan was not reusable with expected options: %#v", options)
	}
	assertTransportNeutralParams(t, options[0].params)

	if err := compiled.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := env.client.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if err := env.server.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
	env.shutdown = true
	plan.mu.Lock()
	planCloseCalls := plan.closeCalls
	plan.mu.Unlock()
	if planCloseCalls != 1 {
		t.Fatalf("Wire closed API plan %d times", planCloseCalls)
	}
	runtime.mu.Lock()
	runtimeCloseCalls := runtime.closeCalls
	runtime.mu.Unlock()
	if runtimeCloseCalls != 0 {
		t.Fatalf("Wire closed borrowed API runtime %d times", runtimeCloseCalls)
	}
	if _, err := runtime.Run(context.Background(), api.NewAnonymousSource("RETURN 1")); err != nil {
		t.Fatalf("borrowed runtime is no longer usable: %v", err)
	}
}

func TestServerShutdownClosesOwnedResourcesWithoutClosingRuntime(t *testing.T) {
	started := make(chan struct{})
	session := &apiSessionSpy{run: func(ctx context.Context) (api.Output, error) {
		close(started)
		<-ctx.Done()

		return api.Output{}, ctx.Err()
	}}
	plan := &apiPlanSpy{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
		return session, nil
	}}
	runtime := &apiRuntimeSpy{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}
	env := newIntegrationEnv(t, runtime)
	compiled, err := env.client.Compile(context.Background(), api.Source{Name: "shutdown.fql", Content: "RETURN 1"}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Execute(context.Background(), nil, client.ExecuteOptions{}); err != nil {
		t.Fatal(err)
	}
	<-started

	if err := env.server.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
	env.shutdown = true

	session.mu.Lock()
	sessionCloseCalls := session.closeCalls
	session.mu.Unlock()
	if sessionCloseCalls != 1 {
		t.Fatalf("shutdown closed API session %d times", sessionCloseCalls)
	}
	plan.mu.Lock()
	planCloseCalls := plan.closeCalls
	plan.mu.Unlock()
	if planCloseCalls != 1 {
		t.Fatalf("shutdown closed API plan %d times", planCloseCalls)
	}
	runtime.mu.Lock()
	runtimeCloseCalls := runtime.closeCalls
	runtime.mu.Unlock()
	if runtimeCloseCalls != 0 {
		t.Fatalf("shutdown closed borrowed API runtime %d times", runtimeCloseCalls)
	}
	if _, err := runtime.Run(context.Background(), api.NewAnonymousSource("RETURN 1")); err != nil {
		t.Fatalf("borrowed runtime is no longer usable: %v", err)
	}
}

func TestNewServerRejectsNilAndTypedNilRuntime(t *testing.T) {
	var typedNil *apiRuntimeSpy
	for name, runtime := range map[string]api.Runtime{
		"nil interface": nil,
		"typed nil":     typedNil,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := server.NewServer(runtime); err == nil || !strings.Contains(err.Error(), "runtime") {
				t.Fatalf("unexpected nil runtime result: %v", err)
			}
		})
	}
}

func TestGenericRuntimeFailuresAreStructuredAndSanitized(t *testing.T) {
	secret := errors.New("runtime compiler secret")
	runtime := &apiRuntimeSpy{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return nil, secret
	}}
	env := newIntegrationEnv(t, runtime)
	_, err := env.client.Compile(context.Background(), api.Source{Content: "broken"}, client.CompileOptions{})
	var wireErr *client.Error
	if !errors.As(err, &wireErr) || wireErr.Category != failure.CategoryCompilation {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if wireErr.Message != "compilation failed" || strings.Contains(wireErr.Message, "secret") || len(wireErr.Diagnostics) != 0 {
		t.Fatalf("runtime details escaped generic fallback: %#v", wireErr)
	}
}

func TestPortableDiagnosticsCrossImmediateAndAsynchronousFailures(t *testing.T) {
	values := testDiagnostics()

	t.Run("compile status", func(t *testing.T) {
		runtime := &apiRuntimeSpy{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return nil, errors.Join(errors.New("runtime compiler secret"), values)
		}}
		env := newIntegrationEnv(t, runtime)

		_, err := env.client.Compile(context.Background(), api.Source{Name: "query.fql", Content: "RETURN"}, client.CompileOptions{})
		var wireErr *client.Error
		if !errors.As(err, &wireErr) || wireErr.Category != failure.CategoryCompilation {
			t.Fatalf("unexpected compile error: %v", err)
		}
		if wireErr.Message != "compilation failed" || strings.Contains(wireErr.Message, "secret") ||
			!reflect.DeepEqual(wireErr.Diagnostics, values) {
			t.Fatalf("portable compile diagnostics changed: %#v", wireErr)
		}
	})

	t.Run("execution failure", func(t *testing.T) {
		plan := &apiPlanSpy{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
			return &apiSessionSpy{run: func(context.Context) (api.Output, error) {
				return api.Output{ContentType: "text/plain", Content: []byte("partial")},
					errors.Join(errors.New("runtime execution secret"), values)
			}}, nil
		}}
		runtime := &apiRuntimeSpy{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
			return plan, nil
		}}
		env := newIntegrationEnv(t, runtime)

		compiled, err := env.client.Compile(context.Background(), api.Source{Name: "query.fql", Content: "RETURN"}, client.CompileOptions{})
		if err != nil {
			t.Fatal(err)
		}
		execution, err := compiled.Execute(context.Background(), nil, client.ExecuteOptions{})
		if err != nil {
			t.Fatal(err)
		}

		output, err := execution.Wait(testContext(t))
		var terminalFailure *failure.Failure
		if !errors.As(err, &terminalFailure) || terminalFailure.Category != failure.CategoryExecution {
			t.Fatalf("unexpected execution failure: %v", err)
		}
		if terminalFailure.Message != "runtime operation failed" || strings.Contains(terminalFailure.Message, "secret") ||
			!reflect.DeepEqual(terminalFailure.Diagnostics, values) {
			t.Fatalf("portable execution diagnostics changed: %#v", terminalFailure)
		}
		if output.ContentType != "text/plain" || string(output.Content) != "partial" {
			t.Fatalf("partial encoded output changed: %#v", output)
		}
	})
}

func testDiagnostics() diagnostics.Diagnostics {
	return diagnostics.Diagnostics{{
		Kind:    diagnostics.TypeError,
		Message: "expected an expression",
		Source:  source.New("query.fql", "RETURN"),
		Annotations: []diagnostics.Annotation{{
			Range: source.Range{
				Location: source.Location{Position: source.Position{Line: 1, Column: 6}, SourceName: "query.fql"},
				Span:     source.Span{Start: 6, End: 6},
			},
			Message: "expression is missing",
			Primary: true,
		}},
		Hint: "provide a value",
		Note: "RETURN requires an expression",
	}}
}

func TestMessageLimitsRemainAtTheGRPCBoundary(t *testing.T) {
	plan := &apiPlanSpy{newSession: func(context.Context, apiSessionOptions) (api.Session, error) {
		return &apiSessionSpy{run: func(context.Context) (api.Output, error) {
			return api.Output{ContentType: "application/json", Content: []byte(strings.Repeat("x", 2048))}, nil
		}}, nil
	}}
	runtime := &apiRuntimeSpy{compile: func(context.Context, api.Source, bool) (api.Plan, error) {
		return plan, nil
	}}
	limits := server.DefaultServerLimits()
	limits.MaxOutboundMessageBytes = 1024
	env := newIntegrationEnv(t, runtime, server.WithServerLimits(limits))

	streamCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := wirev1.NewRuntimeServiceClient(env.conn).Connect(streamCtx, &wirev1.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}
	handshake, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	_, err = wirev1.NewPlanServiceClient(env.conn).Compile(context.Background(), &wirev1.CompileRequest{
		ConnectionId: handshake.GetConnectionId(),
		Source:       &wirev1.Source{Content: "RETURN 1 //" + string(make([]byte, 4<<20))},
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unexpected inbound message result: %v", err)
	}

	compiled, err := env.client.Compile(context.Background(), api.Source{Content: "large"}, client.CompileOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := compiled.Execute(context.Background(), nil, client.ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := execution.Wait(testContext(t)); status.Code(err) != codes.ResourceExhausted {
		var wireErr *client.Error
		if !errors.As(err, &wireErr) || status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("unexpected outbound message result: %v", err)
		}
	}
}

func (r *apiRuntimeSpy) Run(context.Context, api.Source, ...api.SessionOption) (api.Output, error) {
	return api.Output{ContentType: "application/json", Content: []byte("1")}, nil
}

func (r *apiRuntimeSpy) Compile(ctx context.Context, src api.Source, _ ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, false)
}

func (r *apiRuntimeSpy) CompileDebug(ctx context.Context, src api.Source, _ ...api.PlanOption) (api.Plan, error) {
	return r.compilePlan(ctx, src, true)
}

func (r *apiRuntimeSpy) compilePlan(ctx context.Context, src api.Source, debug bool) (api.Plan, error) {
	r.mu.Lock()
	r.sources = append(r.sources, src)
	r.debug = append(r.debug, debug)
	compile := r.compile
	r.mu.Unlock()

	if compile == nil {
		return &apiPlanSpy{}, nil
	}

	return compile(ctx, src, debug)
}

func (r *apiRuntimeSpy) Close() error {
	r.mu.Lock()
	r.closeCalls++
	r.mu.Unlock()

	return nil
}

func (p *apiPlanSpy) Params() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.params...)
}

func (p *apiPlanSpy) NewSession(ctx context.Context, options ...api.SessionOption) (api.Session, error) {
	configured := apiSessionOptions{params: make(map[string]any)}
	for _, option := range options {
		if err := option(&configured); err != nil {
			return nil, err
		}
	}
	p.mu.Lock()
	p.sessionOptions = append(p.sessionOptions, configured.clone())
	newSession := p.newSession
	p.mu.Unlock()

	if newSession == nil {
		return &apiSessionSpy{}, nil
	}

	return newSession(ctx, configured)
}

func (p *apiPlanSpy) NewDebugSession(context.Context, ...api.SessionOption) (debugger.Session, error) {
	return nil, errors.New("debug session is not configured")
}

func (p *apiPlanSpy) Close() error {
	p.mu.Lock()
	p.closeCalls++
	p.mu.Unlock()

	return nil
}

func (s *apiSessionSpy) Run(ctx context.Context) (api.Output, error) {
	if s.run == nil {
		return api.Output{}, nil
	}

	return s.run(ctx)
}

func (s *apiSessionSpy) Close() error {
	s.mu.Lock()
	s.closeCalls++
	closeSession := s.close
	s.mu.Unlock()

	if closeSession == nil {
		return nil
	}

	return closeSession()
}

func (o *apiSessionOptions) SetParam(name string, value any) error {
	o.params[name] = value

	return nil
}

func (o *apiSessionOptions) SetParams(values map[string]any) error {
	for name, value := range values {
		o.params[name] = value
	}

	return nil
}

func (o *apiSessionOptions) SetOutputContentType(contentType string) error {
	o.contentType = contentType

	return nil
}

func (o apiSessionOptions) clone() apiSessionOptions {
	params := make(map[string]any, len(o.params))
	for name, value := range o.params {
		params[name] = value
	}
	o.params = params

	return o
}

func newIntegrationEnv(t *testing.T, runtime api.Runtime, options ...server.ServerOption) *integrationEnv {
	t.Helper()
	server, err := server.NewServer(runtime, options...)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(8 << 20)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(context.Background(), listener) }()
	conn, err := grpc.NewClient(
		"passthrough:///ferret-wire-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	wireClient, err := client.New(testContext(t), conn)
	if err != nil {
		t.Fatal(err)
	}
	env := &integrationEnv{server: server, listener: listener, conn: conn, client: wireClient, serveErr: serveErr}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := wireClient.Close(ctx); err != nil && !errors.Is(err, client.ErrClosed) && !env.shutdown {
			t.Errorf("client cleanup failed: %v", err)
		}
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("server cleanup failed: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Errorf("transport cleanup failed: %v", err)
		}
		select {
		case err := <-serveErr:
			if err != nil {
				t.Errorf("Serve returned an error: %v", err)
			}
		case <-ctx.Done():
			t.Errorf("Serve did not settle: %v", ctx.Err())
		}
	})

	return env
}

func assertTransportNeutralParams(t *testing.T, values map[string]any) {
	t.Helper()
	input, ok := values["input"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected parameter map: %#v", values)
	}
	checks := map[string]any{
		"none": nil, "boolean": true, "integer": int64(7), "float": 3.5,
		"string": "wire", "binary": []byte{1, 2}, "array": []any{"one", int64(2)},
	}
	for name, want := range checks {
		if !reflect.DeepEqual(input[name], want) {
			t.Fatalf("unexpected %s parameter: %#v", name, input[name])
		}
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	return ctx
}
