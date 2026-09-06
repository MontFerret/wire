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
	mu               sync.Mutex
	method           string
	outcome          string
	committed        chan struct{}
	deliver          chan struct{}
	calls            map[string]int
	failures         map[string]error
	responseFailures map[string]error
	methods          []string
}

func (g *allocationResponseGate) Invoke(ctx context.Context, method string, request, response any, options ...grpc.CallOption) error {
	g.mu.Lock()
	g.calls[method]++
	g.methods = append(g.methods, method)
	failure := g.failures[method]
	responseFailure := g.responseFailures[method]
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
	if err != nil {
		return err
	}

	if responseFailure != nil {
		return responseFailure
	}

	if !matched {
		return nil
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
