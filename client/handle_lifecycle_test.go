package client

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/MontFerret/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHandleCloseContinuesAfterFirstCallerCancellation(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	t.Cleanup(func() { unblock(allow) })
	implementation := &handleServer{
		releaseExecutionEntered: entered,
		allowExecutionRelease:   allow,
		releaseExecutionErr:     status.Error(codes.Internal, "retained release failure"),
	}
	connection := startHandleServer(t, implementation)
	client := openHandleClient(t, connection)
	plan, err := client.compileConfigured(testClientContext(t), api.Source{Content: "RETURN 1"}, false, runtimePlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := startTestPlanExecution(testClientContext(t), plan, nil)
	if err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	t.Cleanup(cancelFirst)
	go func() { first <- execution.Close(firstCtx) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("execution close did not reach the server")
	}

	cancelFirst()
	if err := receiveCloseResult(t, first, "first execution close"); !errors.Is(err, context.Canceled) {
		t.Fatalf("first close did not stop waiting after cancellation: %v", err)
	}
	if _, err := execution.Watch(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed execution accepted a watch: %v", err)
	}

	second := make(chan error, 1)
	secondCtx := testClientContext(t)
	go func() { second <- execution.Close(secondCtx) }()
	unblock(allow)
	want := receiveCloseResult(t, second, "later execution close")
	var wireErr *Error
	if !errors.As(want, &wireErr) || status.Code(want) != codes.Internal || wireErr.Message != "retained release failure" {
		t.Fatalf("unexpected release result: %#v", want)
	}
	if err := execution.Close(testClientContext(t)); !errors.As(err, &wireErr) || err.Error() != want.Error() {
		t.Fatalf("repeated close did not retain the first result: %#v", err)
	}

	implementation.mu.Lock()
	releaseCalls := implementation.releaseExecutionCalls
	implementation.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("close issued %d release RPCs", releaseCalls)
	}
}

func TestConcurrentHandleCloseReleasesOnce(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	t.Cleanup(func() { unblock(allow) })
	implementation := &handleServer{
		releaseExecutionEntered: entered,
		allowExecutionRelease:   allow,
	}
	connection := startHandleServer(t, implementation)
	client := openHandleClient(t, connection)
	plan, err := client.compileConfigured(testClientContext(t), api.Source{Content: "RETURN 1"}, false, runtimePlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := startTestPlanExecution(testClientContext(t), plan, nil)
	if err != nil {
		t.Fatal(err)
	}

	const callers = 8
	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		ctx := testClientContext(t)
		go func() {
			<-start
			results <- execution.Close(ctx)
		}()
	}

	close(start)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("execution close did not reach the server")
	}
	unblock(allow)
	for range callers {
		if err := receiveCloseResult(t, results, "concurrent execution close"); err != nil {
			t.Fatalf("concurrent close changed the retained success: %v", err)
		}
	}
	if err := execution.Close(testClientContext(t)); err != nil {
		t.Fatalf("repeated close changed the retained success: %v", err)
	}

	implementation.mu.Lock()
	releaseCalls := implementation.releaseExecutionCalls
	implementation.mu.Unlock()
	if releaseCalls != 1 {
		t.Fatalf("concurrent close issued %d release RPCs", releaseCalls)
	}
}

func TestDescendantCloseDuringAncestorCloseObservesRetainedResult(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	t.Cleanup(func() { unblock(allow) })
	implementation := &handleServer{
		releasePlanEntered: entered,
		allowPlanRelease:   allow,
		releasePlanErr:     status.Error(codes.Internal, "retained ancestor failure"),
	}
	connection := startHandleServer(t, implementation)
	client := openHandleClient(t, connection)
	plan, err := client.compileConfigured(testClientContext(t), api.Source{Content: "RETURN 1"}, true, runtimePlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := startTestPlanExecution(testClientContext(t), plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	debug, err := plan.NewDebugSession(testClientContext(t), runtimeSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}

	planResult := make(chan error, 1)
	planCtx := testClientContext(t)
	go func() { planResult <- plan.Close(planCtx) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("plan close did not reach the server")
	}

	executionResult := make(chan error, 1)
	executionCtx := testClientContext(t)
	go func() { executionResult <- execution.Close(executionCtx) }()
	debugResult := make(chan error, 1)
	debugCtx := testClientContext(t)
	go func() { debugResult <- debug.Close(debugCtx) }()
	waitForCloseStart(t, "execution", execution.close.Started)
	waitForCloseStart(t, "debug session", debug.close.Started)

	calls := implementation.recordedCalls()
	if countCall(calls, call("release-execution", "connection-1", "execution-1")) != 0 ||
		countCall(calls, call("release-debug", "connection-1", "debug-connection-1")) != 0 {
		t.Fatalf("descendants attempted direct cleanup while ancestor was closing: %v", calls)
	}

	unblock(allow)
	want := receiveCloseResult(t, planResult, "plan close")
	var wireErr *Error
	if !errors.As(want, &wireErr) || status.Code(want) != codes.Internal || wireErr.Message != "retained ancestor failure" {
		t.Fatalf("unexpected ancestor release result: %#v", want)
	}
	for name, result := range map[string]<-chan error{
		"execution close":     executionResult,
		"debug session close": debugResult,
	} {
		err := receiveCloseResult(t, result, name)
		var descendantErr *Error
		if !errors.As(err, &descendantErr) || status.Code(err) != status.Code(want) || descendantErr.Message != wireErr.Message {
			t.Fatalf("%s did not observe the retained ancestor result: %#v", name, err)
		}
	}

	calls = implementation.recordedCalls()
	implementation.mu.Lock()
	planReleaseCalls := implementation.releasePlanCalls
	implementation.mu.Unlock()
	if planReleaseCalls != 1 ||
		countCall(calls, call("release-execution", "connection-1", "execution-1")) != 0 ||
		countCall(calls, call("release-debug", "connection-1", "debug-connection-1")) != 0 {
		t.Fatalf("ancestor cleanup issued unexpected release RPCs: %v", calls)
	}
}

func TestDescendantCloseAfterAncestorCloseObservesRetainedResult(t *testing.T) {
	implementation := &handleServer{}
	connection := startHandleServer(t, implementation)
	client := openHandleClient(t, connection)
	plan, err := client.compileConfigured(testClientContext(t), api.Source{Content: "RETURN 1"}, true, runtimePlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := startTestPlanExecution(testClientContext(t), plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	debug, err := plan.NewDebugSession(testClientContext(t), runtimeSessionOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if err := plan.Close(testClientContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := execution.Watch(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("execution survived plan close: %v", err)
	}
	if err := debug.Start(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("debug session survived plan close: %v", err)
	}
	if err := execution.Close(testClientContext(t)); err != nil {
		t.Fatalf("execution did not observe ancestor close: %v", err)
	}
	if err := debug.Close(testClientContext(t)); err != nil {
		t.Fatalf("debug session did not observe ancestor close: %v", err)
	}

	calls := implementation.recordedCalls()
	implementation.mu.Lock()
	planReleaseCalls := implementation.releasePlanCalls
	implementation.mu.Unlock()
	if planReleaseCalls != 1 ||
		countCall(calls, call("release-execution", "connection-1", "execution-1")) != 0 ||
		countCall(calls, call("release-debug", "connection-1", "debug-connection-1")) != 0 {
		t.Fatalf("descendants duplicated ancestor cleanup: %v", calls)
	}
}

func TestZeroValueHandlesAreClosed(t *testing.T) {
	var plan planHandle
	if _, err := plan.newSession(testClientContext(t), runtimeSessionOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero plan accepted session creation: %v", err)
	}
	if err := plan.Close(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero plan close was not closed: %v", err)
	}

	var execution executionHandle
	if _, err := execution.Watch(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero execution accepted a watch: %v", err)
	}
	if err := execution.Close(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero execution close was not closed: %v", err)
	}

	var debug debugSessionHandle
	if err := debug.Start(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero debug session accepted start: %v", err)
	}
	if err := debug.Close(testClientContext(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero debug close was not closed: %v", err)
	}
}

func receiveCloseResult(t *testing.T, result <-chan error, name string) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not settle", name)

		return nil
	}
}

func waitForCloseStart(t *testing.T, name string, started func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	for !started() {
		select {
		case <-deadline.C:
			t.Fatalf("%s close did not start", name)
		default:
			runtime.Gosched()
		}
	}
}

func unblock(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
