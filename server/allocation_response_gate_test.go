package server_test

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// allocationResponseGate withholds a real, committed RPC response while leaving
// the Connect stream and unrelated RPCs alone. Tests control delivery explicitly.
type allocationResponseGate struct {
	grpc.ClientConnInterface
	mu        sync.Mutex
	method    string
	outcome   string
	committed chan struct{}
	deliver   chan struct{}
	calls     map[string]int
	failures  map[string]error
}

func (g *allocationResponseGate) arm(method, outcome string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.method = method
	g.outcome = outcome
	g.committed = make(chan struct{})
	g.deliver = make(chan struct{})
}

func (g *allocationResponseGate) Invoke(ctx context.Context, method string, request, response any, options ...grpc.CallOption) error {
	g.mu.Lock()
	g.calls[method]++
	failure := g.failures[method]
	matched := method == g.method
	outcome, committed, deliver := g.outcome, g.committed, g.deliver
	if matched {
		g.method = ""
	}
	g.mu.Unlock()
	if failure != nil {
		return failure
	}

	err := g.ClientConnInterface.Invoke(ctx, method, request, response, options...)
	if err != nil || !matched {
		return err
	}

	close(committed)
	select {
	case <-deliver:
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}

	switch outcome {
	case "deadline":
		return status.Error(codes.DeadlineExceeded, "allocation response lost")
	case "unavailable":
		return status.Error(codes.Unavailable, "allocation response lost")
	case "oversized":
		return status.Error(codes.ResourceExhausted, "allocation response exceeds receive limit")
	case "transport internal":
		return status.Error(codes.Internal, "allocation response could not be decoded")
	case "malformed":
		proto.Reset(response.(proto.Message))
	}

	return nil
}

func (g *allocationResponseGate) count(method string) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.calls[method]
}

func (g *allocationResponseGate) fail(method string, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.failures[method] = err
}
